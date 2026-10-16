use std::{collections::HashSet, net::SocketAddr, sync::Arc};

use anyhow::Context;
use buffers::ByteBufOwned;
use futures::{Stream, StreamExt, stream::FuturesUnordered};
use librqbit_core::torrent_metainfo::TorrentMetaV1Info;
use tracing::{Instrument, debug, debug_span};

use crate::{
    peer_admission::{MetadataLease, PeerAdmission},
    peer_connection::PeerConnectionOptions,
    peer_info_reader,
    spawn_utils::BlockingSpawner,
    stream_connect::StreamConnector,
};
use librqbit_core::hash_id::Id20;

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum ReadMetainfoResult<Rx> {
    Found {
        info: TorrentMetaV1Info<ByteBufOwned>,
        info_bytes: ByteBufOwned,
        rx: Rx,
        seen: HashSet<SocketAddr>,
    },
    ChannelClosed {
        #[allow(dead_code)]
        seen: HashSet<SocketAddr>,
    },
}

#[allow(clippy::too_many_arguments)]
pub async fn read_metainfo_from_peer_receiver<A: Stream<Item = SocketAddr> + Unpin>(
    peer_id: Id20,
    info_hash: Id20,
    initial_addrs: Vec<SocketAddr>,
    addrs_stream: A,
    peer_connection_options: Option<PeerConnectionOptions>,
    connector: Arc<StreamConnector>,
    client_name_and_version: String,
    admission: Arc<PeerAdmission>,
) -> ReadMetainfoResult<A> {
    let mut seen = HashSet::<SocketAddr>::new();
    let mut addrs = addrs_stream;

    // Metadata probes are long-lived (a peer may never answer) and mostly
    // fail, so they draw on the session connecting budget and are capped
    // independently of the number of addresses known.  Both numbers come from
    // `PeerAdmissionConfig`, never from a local constant.
    let max_in_flight = admission.config().max_concurrent_metadata_reads();
    let mut in_flight = 0usize;

    let read_info_guarded = move |addr, lease: MetadataLease| {
        let connector = connector.clone();
        let client_name_and_version = client_name_and_version.clone();
        async move {
            // The lease is held for the probe's whole lifetime; dropping the
            // future returns the connecting permit.
            let _lease = lease;
            peer_info_reader::read_metainfo_from_peer(
                addr,
                peer_id,
                info_hash,
                peer_connection_options,
                // This shouldn't be called anyway as we aren't reading/writing to disk, so it's
                // ok not to use a shared one.
                BlockingSpawner::new(1),
                connector,
                client_name_and_version,
            )
            .instrument(debug_span!("read_metainfo_from_peer", ?addr))
            .await
            .with_context(|| format!("error reading metainfo from {addr}"))
        }
    };

    let mut unordered = FuturesUnordered::new();

    // Bounded candidate pool: addresses that cannot take a session slot never
    // get a future created at all.  `push_guarded` takes the slot *before*
    // building the future, so the count can never drift from the number of
    // live probes.
    let push_guarded =
        |addr: SocketAddr, unordered: &mut FuturesUnordered<_>, in_flight: &mut usize| {
            if *in_flight >= max_in_flight {
                return;
            }
            let lease = match admission.try_acquire_metadata() {
                Ok(lease) => lease,
                Err(_) => return,
            };
            *in_flight += 1;
            unordered.push(read_info_guarded(addr, lease));
        };

    for a in initial_addrs {
        seen.insert(a);
        push_guarded(a, &mut unordered, &mut in_flight);
    }

    let mut addrs_completed = false;

    loop {
        if addrs_completed && unordered.is_empty() {
            return ReadMetainfoResult::ChannelClosed { seen };
        }

        tokio::select! {
            done = unordered.next(), if !unordered.is_empty() => {
                in_flight = in_flight.saturating_sub(1);
                match done {
                    Some(Ok((info, info_bytes))) => {
                        // Returning drops `unordered`, cancelling every probe
                        // still in flight and releasing their leases.
                        return ReadMetainfoResult::Found { info, info_bytes, seen, rx: addrs };
                    }
                    Some(Err(e)) => {
                        debug!("{:#}", e);
                    },
                    None => unreachable!()
                }
            }

            next_addr = addrs.next(), if !addrs_completed => {
                match next_addr {
                    Some(addr) => {
                        if seen.insert(addr) {
                            push_guarded(addr, &mut unordered, &mut in_flight);
                        }
                        continue;
                    },
                    None => {
                        addrs_completed = true;
                    },
                }
            }
        };
    }
}

#[cfg(test)]
mod tests {
    use dht::{DhtBuilder, Id20};
    use librqbit_core::peer_id::generate_peer_id;

    use super::*;
    use std::{
        str::FromStr,
        sync::{Arc, Once},
    };

    static LOG_INIT: Once = Once::new();

    fn init_logging() {
        #[allow(unused_must_use)]
        LOG_INIT.call_once(|| {
            // pretty_env_logger::try_init();
        })
    }

    #[tokio::test]
    #[ignore]
    async fn read_metainfo_from_dht() {
        init_logging();

        let info_hash = Id20::from_str("cab507494d02ebb1178b38f2e9d7be299c86b862").unwrap();
        let dht = DhtBuilder::new().await.unwrap();

        let peer_rx = dht.get_peers(info_hash, None);
        let peer_id = generate_peer_id(b"-xx1234-");
        match read_metainfo_from_peer_receiver(
            peer_id,
            info_hash,
            Vec::new(),
            peer_rx,
            None,
            Arc::new(StreamConnector::new(Default::default()).await.unwrap()),
            crate::client_name_and_version().to_owned(),
            Arc::new(PeerAdmission::new(Default::default()).unwrap()),
        )
        .await
        {
            ReadMetainfoResult::Found { info, .. } => dbg!(info),
            ReadMetainfoResult::ChannelClosed { .. } => todo!("should not have happened"),
        };
    }
}
