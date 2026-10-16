//! Session-wide peer admission under a real multi-torrent topology.
//!
//! Five torrents download concurrently from one seeding session and share a
//! single, deliberately small session peer budget. Two properties matter and
//! neither is observable from a fake dial source:
//!
//!   * the session never holds more logical peer connections than its budget,
//!     even when every torrent has candidates to dial, and
//!   * no torrent starves: each one still acquires a live peer, because the
//!     shared budget is granted round-robin rather than first-come.

use std::{net::Ipv4Addr, num::NonZeroU32, sync::Arc, time::Duration};

use tracing::{Instrument, error, error_span, info};

use crate::{
    AddTorrentOptions, AddTorrentResponse, ConnectionOptions, ListenerMode, PeerAdmissionConfig,
    Session, SessionOptions, create_torrent,
    limits::LimitsConfig,
    listen::ListenerOptions,
    spawn_utils::BlockingSpawner,
    tests::test_util::{setup_test_logging, wait_until},
};

/// Slow enough that all torrents stay downloading at once, which is what makes
/// the shared session budget contended at all.
const CLIENT_DOWNLOAD_BPS: u32 = 128 * 1024;

const TORRENTS: usize = 5;

/// Chosen so the outbound share of the logical budget is smaller than the
/// number of torrents: admission keeps inbound headroom out of the budget, and
/// at this size that headroom dominates.  The torrents therefore cannot all be
/// live at once, and the scheduler has to rotate between them.  The exact share
/// is deliberately not asserted - only that it is below [`TORRENTS`] and that
/// every torrent is still served.
const SESSION_PEER_LIMIT: usize = 10;
const TORRENT_CONNECT_LIMIT: usize = 2;
const SESSION_CONNECT_LIMIT: usize = 8;

fn admission() -> PeerAdmissionConfig {
    PeerAdmissionConfig {
        session_peer_limit: SESSION_PEER_LIMIT,
        torrent_connect_limit: TORRENT_CONNECT_LIMIT,
        session_connect_limit: SESSION_CONNECT_LIMIT,
        outbound_connect_rate: 50,
    }
}

fn opts(port: u16, admission: PeerAdmissionConfig, span: tracing::Span) -> SessionOptions {
    SessionOptions {
        dht: None,
        listen: Some(ListenerOptions {
            mode: ListenerMode::TcpOnly,
            listen_addr: (Ipv4Addr::LOCALHOST, port).into(),
            ..Default::default()
        }),
        connect: Some(ConnectionOptions {
            enable_tcp: true,
            ..Default::default()
        }),
        peer_admission: admission,
        disable_local_service_discovery: true,
        root_span: Some(span),
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn multi_torrent_shares_the_session_budget_without_starving_any_torrent() {
    setup_test_logging();
    let timeout = std::env::var("E2E_TIMEOUT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(180u64);

    tokio::time::timeout(
        Duration::from_secs(timeout),
        run().instrument(error_span!(parent: None, "multi_torrent_budget")),
    )
    .await
    .map_err(|_| error!("multi-torrent budget test timed out"))
    .expect("multi-torrent budget test timed out");
}

async fn run() {
    let piece_length: u32 = 32768;
    let file_length: usize = 512 * 1024;

    // One seeding session holding several independent torrents.
    let provider_root = tempfile::TempDir::with_prefix("rqbit_budget_provider").expect("tempdir");
    let provider = Arc::new(
        Session::new_with_opts(
            provider_root.path().join("data"),
            opts(
                15400,
                PeerAdmissionConfig::default(),
                error_span!(parent: None, "provider"),
            ),
        )
        .await
        .expect("provider session"),
    );
    let provider_addr = provider.listen_addr().expect("provider addr");

    let mut torrents = Vec::new();
    for i in 0..TORRENTS {
        let content_dir = provider_root.path().join(format!("t{i}"));
        std::fs::create_dir_all(&content_dir).expect("content dir");
        // One file with deterministic, per-torrent distinct content.
        let file_path = content_dir.join("payload.bin");
        let mut payload = vec![0u8; file_length];
        for (n, byte) in payload.iter_mut().enumerate() {
            *byte = ((n + i * 7) % 251) as u8;
        }
        std::fs::write(&file_path, &payload).expect("write payload");

        let torrent_file = create_torrent(
            &content_dir,
            crate::CreateTorrentOptions {
                piece_length: Some(piece_length),
                ..Default::default()
            },
            &BlockingSpawner::new(1),
        )
        .await
        .expect("create torrent");

        let bytes = torrent_file.as_bytes().expect("torrent bytes");
        let handle = match provider
            .add_torrent(
                crate::AddTorrent::TorrentFileBytes(bytes),
                Some(AddTorrentOptions {
                    overwrite: true,
                    output_folder: Some(content_dir.to_str().unwrap().to_owned()),
                    ..Default::default()
                }),
            )
            .await
            .expect("provider add")
        {
            AddTorrentResponse::Added(_, h) | AddTorrentResponse::AlreadyManaged(_, h) => h,
            AddTorrentResponse::ListOnly(_) => panic!("unexpected list-only"),
        };
        torrents.push((torrent_file, handle));
    }

    // The provider must actually hold the data before anyone dials it.
    wait_until(
        || {
            if torrents.iter().all(|(_, h)| h.stats().finished) {
                Ok(())
            } else {
                anyhow::bail!("provider torrents still initializing")
            }
        },
        Duration::from_secs(60),
    )
    .await
    .expect("provider must finish verifying");

    // The downloading session shares one small budget across all torrents.
    let client_root = tempfile::TempDir::with_prefix("rqbit_budget_client").expect("tempdir");
    let client = Arc::new(
        Session::new_with_opts(
            client_root.path().join("data"),
            SessionOptions {
                ratelimits: LimitsConfig {
                    download_bps: NonZeroU32::new(CLIENT_DOWNLOAD_BPS),
                    upload_bps: None,
                },
                ..opts(15401, admission(), error_span!(parent: None, "client"))
            },
        )
        .await
        .expect("client session"),
    );

    let mut handles = Vec::new();
    for (i, (torrent_file, _)) in torrents.iter().enumerate() {
        let bytes = torrent_file.as_bytes().expect("torrent bytes");
        let out = client_root.path().join(format!("out{i}"));
        std::fs::create_dir_all(&out).expect("out dir");
        let handle = match client
            .add_torrent(
                crate::AddTorrent::TorrentFileBytes(bytes),
                Some(AddTorrentOptions {
                    initial_peers: Some(vec![provider_addr]),
                    output_folder: Some(out.to_str().unwrap().to_owned()),
                    overwrite: true,
                    ..Default::default()
                }),
            )
            .await
            .expect("client add")
        {
            AddTorrentResponse::Added(_, h) | AddTorrentResponse::AlreadyManaged(_, h) => h,
            AddTorrentResponse::ListOnly(_) => panic!("unexpected list-only"),
        };
        handles.push(handle);
    }

    // Sample the session aggregate while the downloads run: the budget must
    // hold at every instant, and every torrent must eventually get a peer.
    let mut peak_logical = 0usize;
    let mut peak_live_torrents = 0usize;
    let mut ever_live = vec![false; TORRENTS];
    let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
    loop {
        let stats = client.stats.peers.snapshot();
        let logical = stats.live as usize + stats.connecting as usize;
        peak_logical = peak_logical.max(logical);
        let mut live_now = 0usize;
        for (i, handle) in handles.iter().enumerate() {
            if handle
                .stats()
                .live
                .as_ref()
                .is_some_and(|l| l.snapshot.peer_stats.live >= 1)
            {
                ever_live[i] = true;
                live_now += 1;
            }
        }
        peak_live_torrents = peak_live_torrents.max(live_now);
        if ever_live.iter().all(|seen| *seen) || tokio::time::Instant::now() > deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    info!(
        peak_logical,
        peak_live_torrents,
        budget = SESSION_PEER_LIMIT,
        "session admission peak"
    );
    assert!(
        peak_logical <= SESSION_PEER_LIMIT,
        "session held {peak_logical} logical peers against a budget of {SESSION_PEER_LIMIT}"
    );
    // The outbound share of this budget cannot seat all five torrents, so the
    // run only proves anything if the torrents really did contend for it.
    assert!(
        peak_live_torrents >= 2 && peak_live_torrents < TORRENTS,
        "expected the torrents to contend for the shared budget, saw at most \
         {peak_live_torrents} of {TORRENTS} live at once"
    );
    assert!(
        ever_live.iter().all(|seen| *seen),
        "every torrent must acquire a peer under the shared budget, got {ever_live:?}"
    );

    // Downloads must actually progress, not merely connect.
    let progressed = handles
        .iter()
        .filter(|h| {
            h.stats()
                .live
                .as_ref()
                .is_some_and(|l| l.snapshot.downloaded_and_checked_pieces > 0)
        })
        .count();
    assert!(
        progressed > 0,
        "at least one torrent must have verified pieces from the provider"
    );

    // Sample the aggregate peak a few more times while transfers are live:
    // the earlier loop breaks as soon as every torrent has been seen with a
    // peer, so keep asserting the invariant for a short window afterwards.
    let hold_until = tokio::time::Instant::now() + Duration::from_secs(2);
    while tokio::time::Instant::now() < hold_until {
        let stats = client.stats.peers.snapshot();
        let logical = stats.live as usize + stats.connecting as usize;
        assert!(
            logical <= SESSION_PEER_LIMIT,
            "session held {logical} logical peers against a budget of {SESSION_PEER_LIMIT}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
