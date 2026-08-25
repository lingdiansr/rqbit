use std::{
    sync::{
        Arc,
        atomic::{AtomicU32, AtomicU64, Ordering},
    },
    time::Duration,
};

use backon::{BackoffBuilder, ExponentialBackoff, ExponentialBuilder};

#[derive(Default, Debug)]
pub(crate) struct PeerCountersAtomic {
    pub fetched_bytes: AtomicU64,
    pub uploaded_bytes: AtomicU64,
    pub total_time_connecting_ms: AtomicU64,
    pub incoming_connections: AtomicU32,
    pub outgoing_connection_attempts: AtomicU32,
    pub outgoing_connections: AtomicU32,
    pub errors: AtomicU32,
    pub fetched_chunks: AtomicU32,
    pub downloaded_and_checked_pieces: AtomicU32,
    pub downloaded_and_checked_bytes: AtomicU64,
    pub total_piece_download_ms: AtomicU64,
    pub times_stolen_from_me: AtomicU32,
    pub times_i_stole: AtomicU32,
    /// Cumulative count of pieces whose hash failed to validate, used to
    /// decide whether to disconnect the peer (aligned with transmission
    /// `MaxBadPiecesPerPeer`).
    pub bad_pieces: AtomicU32,
}

impl PeerCountersAtomic {
    pub(crate) fn on_piece_completed(&self, piece_len: u64, elapsed: Duration) {
        #[allow(clippy::cast_possible_truncation)]
        let elapsed = elapsed.as_millis() as u64;
        self.total_piece_download_ms
            .fetch_add(elapsed, Ordering::Release);
        self.downloaded_and_checked_pieces
            .fetch_add(1, Ordering::Release);
        self.downloaded_and_checked_bytes
            .fetch_add(piece_len, Ordering::Relaxed);
    }

    pub(crate) fn average_piece_download_time(&self) -> Option<Duration> {
        let downloaded_pieces = self.downloaded_and_checked_pieces.load(Ordering::Acquire);
        let total_download_time = self.total_piece_download_ms.load(Ordering::Acquire);
        if total_download_time == 0 || downloaded_pieces == 0 {
            return None;
        }
        Some(Duration::from_millis(
            total_download_time / downloaded_pieces as u64,
        ))
    }
}

fn backoff(cfg: Option<&crate::PeerBackoffConfig>) -> ExponentialBackoff {
    let mut builder = match cfg {
        None => ExponentialBuilder::new()
            .with_min_delay(Duration::from_secs(10))
            .with_factor(6.)
            .with_max_delay(Duration::from_secs(3600))
            .with_total_delay(Some(Duration::from_secs(86400))),
        Some(c) => ExponentialBuilder::new()
            .with_min_delay(c.min_delay)
            .with_factor(c.factor)
            .with_max_delay(c.max_delay)
            .with_total_delay(c.total_delay),
    };
    builder = builder.with_jitter();
    builder.without_max_times().build()
}

#[derive(Debug)]
pub(crate) struct PeerStats {
    pub counters: Arc<PeerCountersAtomic>,
    pub backoff: ExponentialBackoff,
    backoff_config: Option<crate::PeerBackoffConfig>,
}

impl Default for PeerStats {
    fn default() -> Self {
        Self::with_backoff_config(None)
    }
}

impl PeerStats {
    pub(crate) fn with_backoff_config(backoff_config: Option<crate::PeerBackoffConfig>) -> Self {
        let backoff = backoff(backoff_config.as_ref());
        Self {
            counters: Arc::new(Default::default()),
            backoff,
            backoff_config,
        }
    }

    pub fn reset_backoff(&mut self) {
        self.backoff = backoff(self.backoff_config.as_ref());
    }
}
