use std::sync::atomic::{AtomicU32, AtomicU64};

#[derive(Default, Debug)]
pub struct AtomicStats {
    pub have_bytes: AtomicU64,
    pub downloaded_and_checked_bytes: AtomicU64,
    pub downloaded_and_checked_pieces: AtomicU64,
    pub uploaded_bytes: AtomicU64,
    pub fetched_bytes: AtomicU64,
    pub total_piece_download_ms: AtomicU64,
    /// Accepted, valid upload requests, cumulative for this live instance.
    pub upload_requests: AtomicU64,
    /// Upload requests enqueued but not yet handed to the writer.
    pub upload_queue_depth: AtomicU32,
    /// Live peers that took payload from us within the recent-activity window.
    pub uploading_peers: AtomicU32,
}
