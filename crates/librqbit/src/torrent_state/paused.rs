use std::{collections::HashSet, sync::Arc};

use crate::{
    chunk_tracker::{ChunkTracker, HaveNeededSelected},
    peer_admission::TorrentAdmission,
    type_aliases::FileStorage,
};

use super::{ManagedTorrentShared, TorrentMetadata, streaming::TorrentStreams};

pub struct TorrentStatePaused {
    pub(crate) shared: Arc<ManagedTorrentShared>,
    pub(crate) metadata: Arc<TorrentMetadata>,
    pub(crate) files: FileStorage,
    pub(crate) chunk_tracker: ChunkTracker,
    pub(crate) streams: Arc<TorrentStreams>,
    /// This torrent's per-torrent peer budgets (live cap + outbound
    /// connecting quota).  Survives pause/unpause, so a torrent that is
    /// paused and resumed keeps the same budget objects.
    pub(crate) admission: Arc<TorrentAdmission>,
}

impl TorrentStatePaused {
    /// Build the budgets for a torrent from its options.  The per-torrent
    /// quota is shared with the session's dial scheduler, which is why the
    /// paused state has to own it: the state is what the scheduler holds on
    /// to while the torrent is live.
    pub(crate) fn new_admission(shared: &ManagedTorrentShared) -> Arc<TorrentAdmission> {
        Arc::new(TorrentAdmission::new(
            shared.options.peer_limit.unwrap_or(128),
            shared.options.torrent_connect_limit,
        ))
    }
    pub(crate) fn update_only_files(&mut self, only_files: &HashSet<usize>) -> anyhow::Result<()> {
        self.chunk_tracker
            .update_only_files(&self.metadata.file_infos, only_files)?;
        Ok(())
    }

    pub(crate) fn hns(&self) -> &HaveNeededSelected {
        self.chunk_tracker.get_hns()
    }
}
