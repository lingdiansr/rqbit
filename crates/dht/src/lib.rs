mod bprotocol;
mod dht;
mod error;
mod peer_store;
mod persistence;
mod routing_table;
mod utils;

use std::sync::Arc;
use std::time::Duration;

pub use error::{Error, Result};

pub use crate::dht::DhtStats;
pub use crate::dht::{DhtConfig, DhtState, RequestPeersStream};
pub use librqbit_core::hash_id::Id20;
pub use persistence::{DhtPersistenceConfig, PersistentDht, dht_listen_addr};

pub type Dht = Arc<DhtState>;

// How long do we wait for a response from a DHT node.
pub(crate) const RESPONSE_TIMEOUT: Duration = Duration::from_secs(10);
// TODO: Not sure if we should re-query tbh.
pub(crate) const REQUERY_INTERVAL: Duration = Duration::from_secs(60);
// After how long we consider a routing table node questionable.
pub(crate) const INACTIVITY_TIMEOUT: Duration = Duration::from_secs(15 * 60);

pub struct DhtBuilder {}

impl DhtBuilder {
    #[allow(clippy::new_ret_no_self)]
    pub async fn new() -> crate::Result<Dht> {
        DhtState::new().await
    }

    pub async fn with_config(config: DhtConfig<'_>) -> crate::Result<Dht> {
        DhtState::with_config(config).await
    }
}

// Bootstrap nodes: upstream only had transmissionbt + libtorrent.org (the
// latter frequently unreachable from some networks, e.g. CN). Add the
// mainstream BitTorrent/uTorrent/BitComet routers so DHT bootstraps reliably
// from multiple nodes; critical for pure-DHT magnets (no tracker in URL).
pub static DHT_BOOTSTRAP: &[&str] = &[
    "router.bittorrent.com:6881",
    "router.utorrent.com:6881",
    "router.bitcomet.net:6881",
    "dht.transmissionbt.com:6881",
    "dht.libtorrent.org:25401",
];
