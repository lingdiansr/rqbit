//! Bounded, per-peer-fair upload request queue.
//!
//! Upstream keeps a torrent-level **unbounded** FIFO of upload requests: one
//! misbehaving peer can queue an arbitrary number of chunk requests and starve
//! every other peer, and `Cancel` / `NotInterested` are ignored entirely.
//!
//! This module replaces that FIFO with:
//!
//! * a hard per-peer cap ([`MAX_PENDING_PER_PEER`]) and torrent cap
//!   ([`MAX_PENDING_PER_TORRENT`]) — exceeding the per-peer cap is treated as
//!   request flooding, not silently absorbed;
//! * round-robin service across peers, so a peer with a long backlog does not
//!   monopolise the writer;
//! * request-key dedup, so a repeated `Request` is queued once;
//! * real `Cancel` / `NotInterested` handling;
//! * a generation per queued entry, so a request cancelled or invalidated by a
//!   reconnect is never written.
//!
//! Only the chunk descriptor (piece index / begin / length) is queued — the
//! payload is read later by the existing writer, so nothing is copied early.

use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;

/// Most outstanding upload requests kept for a single peer.
///
/// Covers queued, scheduled and in-flight-with-the-writer requests together.
pub(crate) const MAX_PENDING_PER_PEER: usize = 128;

/// Most outstanding upload requests kept across one torrent.
pub(crate) const MAX_PENDING_PER_TORRENT: usize = 4096;

/// Identity of one requested chunk.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct RequestKey {
    pub piece: u32,
    pub begin: u32,
    pub length: u32,
}

/// One queued upload request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct QueuedRequest {
    pub key: RequestKey,
    /// Bumped whenever the peer's connection identity changes, so a request
    /// queued for a previous connection is never written to a new one.
    pub generation: u64,
    pub peer: SocketAddr,
}

/// What happened to an enqueue attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EnqueueOutcome {
    /// Queued for the first time.
    Queued,
    /// The same chunk was already queued for this peer; nothing to do.
    Duplicate,
    /// The peer already has the maximum outstanding requests — this is
    /// request flooding, and the caller should disconnect the peer.
    PeerBacklogExceeded,
    /// The torrent-wide backlog is full.  A real error: the request is NOT
    /// queued and is NOT silently dropped into an unbounded wait.
    TorrentBacklogExceeded,
}

#[derive(Debug, Default)]
struct PeerQueue {
    /// FIFO of this peer's requests, in arrival order.
    queue: VecDeque<QueuedRequest>,
    /// Dedup set mirroring `queue`.
    keys: HashSet<RequestKey>,
}

/// Bounded, round-robin upload request queue for one torrent.
#[derive(Debug, Default)]
pub(crate) struct UploadQueue {
    /// Per-peer queues, by peer address.
    peers: HashMap<SocketAddr, PeerQueue>,
    /// Round-robin cursor over the peers that have work.
    rotation: VecDeque<SocketAddr>,
    /// Total queued requests across all peers.
    total: usize,
}

impl UploadQueue {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Outstanding requests across all peers.
    ///
    /// Consumed by the queue's tests today and by the upload-backlog
    /// statistic in step 7.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn total_pending(&self) -> usize {
        self.total
    }

    /// Queue a request, enforcing both caps and dedup.
    pub(crate) fn enqueue(&mut self, req: QueuedRequest) -> EnqueueOutcome {
        let entry = self.peers.entry(req.peer).or_default();

        if entry.keys.contains(&req.key) {
            return EnqueueOutcome::Duplicate;
        }
        if entry.queue.len() >= MAX_PENDING_PER_PEER {
            return EnqueueOutcome::PeerBacklogExceeded;
        }
        if self.total >= MAX_PENDING_PER_TORRENT {
            return EnqueueOutcome::TorrentBacklogExceeded;
        }

        entry.keys.insert(req.key);
        entry.queue.push_back(req);
        self.total += 1;
        if !self.rotation.iter().any(|p| *p == req.peer) {
            self.rotation.push_back(req.peer);
        }
        EnqueueOutcome::Queued
    }

    /// Take the next request to serve, rotating across peers.
    pub(crate) fn pop_next(&mut self) -> Option<QueuedRequest> {
        while let Some(peer) = self.rotation.pop_front() {
            let Some(entry) = self.peers.get_mut(&peer) else {
                continue;
            };
            let Some(req) = entry.queue.pop_front() else {
                continue;
            };
            entry.keys.remove(&req.key);
            self.total -= 1;
            if !entry.queue.is_empty() {
                // Keep this peer in the rotation, at the back.
                self.rotation.push_back(peer);
            } else {
                self.peers.remove(&peer);
            }
            return Some(req);
        }
        None
    }

    /// Cancel one specific request (`Cancel` message).
    ///
    /// Removes the request only if it has not been handed to the writer yet;
    /// a request already being written is allowed to finish.
    pub(crate) fn cancel(&mut self, peer: SocketAddr, key: RequestKey, generation: u64) -> bool {
        let Some(entry) = self.peers.get_mut(&peer) else {
            return false;
        };
        let Some(pos) = entry
            .queue
            .iter()
            .position(|r| r.key == key && r.generation == generation)
        else {
            return false;
        };
        entry.queue.remove(pos);
        entry.keys.remove(&key);
        self.total -= 1;
        if entry.queue.is_empty() {
            self.peers.remove(&peer);
            self.rotation.retain(|p| *p != peer);
        }
        true
    }

    /// Drop every pending request for a peer (`NotInterested`, disconnect).
    pub(crate) fn drop_peer(&mut self, peer: SocketAddr) -> usize {
        let removed = self.peers.remove(&peer).map_or(0, |p| p.queue.len());
        self.total -= removed;
        self.rotation.retain(|p| *p != peer);
        removed
    }

    /// Drop pending requests whose generation is stale (reconnect / pause).
    pub(crate) fn drop_stale_generations(&mut self, peer: SocketAddr, current: u64) -> usize {
        let Some(entry) = self.peers.get_mut(&peer) else {
            return 0;
        };
        let before = entry.queue.len();
        entry.queue.retain(|r| r.generation == current);
        entry.keys = entry.queue.iter().map(|r| r.key).collect();
        let removed = before - entry.queue.len();
        self.total -= removed;
        if entry.queue.is_empty() {
            self.peers.remove(&peer);
            self.rotation.retain(|p| *p != peer);
        }
        removed
    }

    /// Forget everything (torrent paused/removed).
    pub(crate) fn clear(&mut self) {
        self.peers.clear();
        self.rotation.clear();
        self.total = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(n: u16) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], n))
    }

    fn u32_of(n: usize) -> u32 {
        u32::try_from(n).expect("small test constant")
    }

    fn key(n: u32) -> RequestKey {
        RequestKey {
            piece: n,
            begin: 0,
            length: 16384,
        }
    }

    fn req(peer: SocketAddr, n: u32) -> QueuedRequest {
        QueuedRequest {
            key: key(n),
            generation: 1,
            peer,
        }
    }

    #[test]
    fn duplicate_requests_are_queued_once() {
        let mut q = UploadQueue::new();
        assert_eq!(q.enqueue(req(addr(1), 1)), EnqueueOutcome::Queued);
        assert_eq!(q.enqueue(req(addr(1), 1)), EnqueueOutcome::Duplicate);
        assert_eq!(q.total_pending(), 1);
    }

    #[test]
    fn per_peer_cap_is_enforced_and_flagged_as_flooding() {
        let mut q = UploadQueue::new();
        let peer = addr(1);
        for n in 0..u32_of(MAX_PENDING_PER_PEER) {
            assert_eq!(q.enqueue(req(peer, n)), EnqueueOutcome::Queued);
        }
        assert_eq!(
            q.enqueue(req(peer, u32_of(MAX_PENDING_PER_PEER))),
            EnqueueOutcome::PeerBacklogExceeded,
            "a peer past its window must be reported, not silently absorbed"
        );
    }

    #[test]
    fn torrent_cap_applies_across_peers() {
        let mut q = UploadQueue::new();
        let mut queued = 0usize;
        'outer: for p in 0..64u16 {
            for n in 0..u32_of(MAX_PENDING_PER_PEER) {
                match q.enqueue(req(addr(p + 1), n)) {
                    EnqueueOutcome::Queued => queued += 1,
                    EnqueueOutcome::TorrentBacklogExceeded => break 'outer,
                    other => panic!("unexpected {other:?}"),
                }
            }
        }
        assert_eq!(queued, MAX_PENDING_PER_TORRENT);
        assert_eq!(q.total_pending(), MAX_PENDING_PER_TORRENT);
    }

    #[test]
    fn service_rotates_between_peers() {
        let mut q = UploadQueue::new();
        let a = addr(1);
        let b = addr(2);
        for n in 0..5 {
            q.enqueue(req(a, n));
            q.enqueue(req(b, 100 + n));
        }
        let mut order = Vec::new();
        while let Some(r) = q.pop_next() {
            order.push(r.peer);
        }
        // Strict alternation: a heavy peer cannot starve a light one.
        for pair in order.chunks(2) {
            assert_ne!(pair[0], pair[1], "peers must alternate, got {order:?}");
        }
        assert!(order.contains(&a));
        assert!(order.contains(&b));
    }

    #[test]
    fn cancel_removes_only_an_unstarted_request() {
        let mut q = UploadQueue::new();
        let peer = addr(1);
        q.enqueue(req(peer, 1));
        q.enqueue(req(peer, 2));
        assert!(q.cancel(peer, key(1), 1));
        assert!(!q.cancel(peer, key(1), 1), "cancelling twice is a no-op");
        assert_eq!(q.total_pending(), 1);
        // The surviving request is the one that was not cancelled.
        assert_eq!(q.pop_next().map(|r| r.key), Some(key(2)));
    }

    #[test]
    fn not_interested_drops_the_peers_pending_work() {
        let mut q = UploadQueue::new();
        let a = addr(1);
        let b = addr(2);
        for n in 0..3 {
            q.enqueue(req(a, n));
            q.enqueue(req(b, 200 + n));
        }
        assert_eq!(q.drop_peer(a), 3);
        assert_eq!(q.total_pending(), 3, "the other peer is untouched");
        while let Some(r) = q.pop_next() {
            assert_eq!(r.peer, b);
        }
    }

    #[test]
    fn stale_generation_requests_are_never_served() {
        let mut q = UploadQueue::new();
        let peer = addr(1);
        q.enqueue(req(peer, 1)); // generation 1
        q.enqueue(QueuedRequest {
            key: key(2),
            generation: 2,
            peer,
        });
        assert_eq!(q.drop_stale_generations(peer, 2), 1);
        assert_eq!(q.pop_next().map(|r| r.generation), Some(2));
    }
}
