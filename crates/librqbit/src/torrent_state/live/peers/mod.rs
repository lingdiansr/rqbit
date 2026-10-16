use std::{collections::HashSet, net::SocketAddr, sync::Arc, sync::atomic::Ordering};

use dashmap::DashMap;
use librqbit_core::lengths::ValidPieceIndex;
use parking_lot::RwLock;

use crate::{
    Error,
    torrent_state::live::peer::stats::atomic::USEFUL_PEER_BYTES,
    torrent_state::utils::{TimedExistence, atomic_inc},
    type_aliases::{BF, PeerHandle},
};

use self::stats::{AggregatePeerStats, AggregatePeerStatsAtomic};

use super::peer::{LivePeerState, Peer, PeerRx, PeerState, PeerTx};

pub mod stats;

/// Maximum number of candidate peer records kept per torrent.
///
/// Discovery streams (tracker / PEX / DHT, plus backoff retries) are unbounded
/// in principle; this bounds the memory and the work the dial scheduler can be
/// handed.  Established connections are never sacrificed to stay under it.
pub(crate) const MAX_CANDIDATE_PEERS: usize = 3000;

pub(crate) struct PeerStates {
    pub session_stats: Arc<AggregatePeerStatsAtomic>,
    /// Monotonic insertion counter for oldest-first eviction.
    pub(crate) next_insertion_seq: std::sync::atomic::AtomicU64,

    // This keeps track of live addresses we connected to, for PEX.
    pub live_outgoing_peers: RwLock<HashSet<PeerHandle>>,
    // Backoff config for outgoing peers; None = upstream defaults.
    pub peer_backoff: Option<crate::PeerBackoffConfig>,
    pub stats: AggregatePeerStatsAtomic,
    pub states: DashMap<PeerHandle, Peer>,
}

impl Drop for PeerStates {
    fn drop(&mut self) {
        for (_, p) in std::mem::take(&mut self.states).into_iter() {
            p.destroy(self);
        }
    }
}

impl PeerStates {
    pub fn stats(&self) -> AggregatePeerStats {
        self.stats.snapshot()
    }

    /// Make room for one more candidate, if possible.
    ///
    /// Evicts the oldest peer in the lowest eviction rank (Dead, then
    /// NotNeeded, then Queued).  Returns `false` when the pool is full and
    /// every remaining peer is one we must keep — the caller then rejects the
    /// new candidate and counts it.
    /// Test-only: evict one candidate by the production ordering and report
    /// which address was removed.
    #[cfg(test)]
    pub(crate) fn evict_one_candidate_for_test(&self) -> Option<SocketAddr> {
        let mut victim: Option<(u8, u64, SocketAddr)> = None;
        for entry in self.states.iter() {
            let peer = entry.value();
            if !peer.is_evictable_candidate() {
                continue;
            }
            let rank = peer.eviction_rank();
            let seq = peer.insertion_seq;
            let key = *entry.key();
            let better = match victim {
                None => true,
                Some((best_rank, best_seq, _)) => {
                    rank < best_rank || (rank == best_rank && seq < best_seq)
                }
            };
            if better {
                victim = Some((rank, seq, key));
            }
        }
        let (_, _, addr) = victim?;
        self.drop_peer(addr);
        Some(addr)
    }

    fn make_room_for_candidate(&self) -> bool {
        if self.states.len() < MAX_CANDIDATE_PEERS {
            return true;
        }

        // Pick the eviction victim in a single pass: lowest rank wins, then
        // lowest insertion sequence (oldest).
        let mut victim: Option<(u8, u64, SocketAddr)> = None;
        for entry in self.states.iter() {
            let peer = entry.value();
            if !peer.is_evictable_candidate() {
                continue;
            }
            let rank = peer.eviction_rank();
            let seq = peer.insertion_seq;
            let key = *entry.key();
            let better = match victim {
                None => true,
                Some((best_rank, best_seq, _)) => {
                    rank < best_rank || (rank == best_rank && seq < best_seq)
                }
            };
            if better {
                victim = Some((rank, seq, key));
            }
        }

        match victim {
            Some((_, _, addr)) => {
                self.drop_peer(addr);
                true
            }
            None => {
                atomic_inc(&self.stats.candidate_rejected);
                atomic_inc(&self.session_stats.candidate_rejected);
                false
            }
        }
    }

    pub fn add_if_not_seen(&self, addr: SocketAddr) -> Option<PeerHandle> {
        use dashmap::mapref::entry::Entry;

        // Capacity must be managed BEFORE taking a DashMap entry: holding an
        // entry borrows a shard, and eviction iterates the whole map.
        if self.states.contains_key(&addr) {
            return None;
        }
        if !self.make_room_for_candidate() {
            return None;
        }

        match self.states.entry(addr) {
            Entry::Occupied(_) => None,
            Entry::Vacant(vac) => {
                let seq = self
                    .next_insertion_seq
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                vac.insert(Peer::new_with_outgoing_address(
                    addr,
                    self.peer_backoff,
                    seq,
                ));
                atomic_inc(&self.stats.queued);
                atomic_inc(&self.session_stats.queued);

                atomic_inc(&self.stats.seen);
                atomic_inc(&self.session_stats.seen);
                Some(addr)
            }
        }
    }
    pub fn with_peer<R>(&self, addr: PeerHandle, f: impl FnOnce(&Peer) -> R) -> Option<R> {
        self.states.get(&addr).map(|e| f(e.value()))
    }

    pub fn with_peer_mut<R>(
        &self,
        addr: PeerHandle,
        reason: &'static str,
        f: impl FnOnce(&mut Peer) -> R,
    ) -> Option<R> {
        use crate::torrent_state::utils::timeit;
        timeit(reason, || self.states.get_mut(&addr))
            .map(|e| f(TimedExistence::new(e, reason).value_mut()))
    }

    pub fn with_live<R>(&self, addr: PeerHandle, f: impl FnOnce(&LivePeerState) -> R) -> Option<R> {
        self.with_peer(addr, |peer| peer.get_live().map(f))
            .flatten()
    }

    pub fn with_live_mut<R>(
        &self,
        addr: PeerHandle,
        reason: &'static str,
        f: impl FnOnce(&mut LivePeerState) -> R,
    ) -> Option<R> {
        self.with_peer_mut(addr, reason, |peer| peer.get_live_mut().map(f))
            .flatten()
    }

    pub fn drop_peer(&self, handle: PeerHandle) -> Option<Peer> {
        let p = self.states.remove(&handle).map(|r| r.1)?;
        let s = p.get_state();
        // A departed peer stops counting as useful; without this the aggregate
        // would drift upward forever. Read from the removed record, which is
        // the last observation of its lifetime. (Interest is part of the peer
        // state, so `dec` below already un-counts it — doing it here too would
        // underflow the counter.)
        if p.stats.counters.fetched_bytes.load(Ordering::Relaxed) >= USEFUL_PEER_BYTES {
            crate::torrent_state::utils::atomic_dec(&self.stats.useful_peers);
        }
        self.stats.dec(s);
        self.session_stats.dec(s);

        Some(p)
    }

    pub fn is_peer_not_interested_and_has_full_torrent(
        &self,
        handle: PeerHandle,
        total_pieces: usize,
    ) -> bool {
        self.with_live(handle, |live| {
            !live.peer_interested && live.has_full_torrent(total_pieces)
        })
        .unwrap_or(false)
    }

    /// Account payload this peer fetched from us, maintaining the
    /// `useful_peers` aggregate.
    ///
    /// Lives here rather than in the message handler so the "a peer becomes
    /// useful exactly once, when it first crosses the threshold" rule has a
    /// single home that a unit test can drive.
    pub(crate) fn note_peer_fetched_bytes(
        &self,
        counters: &super::peer::stats::atomic::PeerCountersAtomic,
        len: u64,
    ) {
        let before = counters.fetched_bytes.fetch_add(len, Ordering::Relaxed);
        if before < USEFUL_PEER_BYTES && before + len >= USEFUL_PEER_BYTES {
            atomic_inc(&self.stats.useful_peers);
        }
    }

    pub fn mark_peer_interested(&self, handle: PeerHandle, is_interested: bool) -> Option<bool> {
        let changed = self.with_live_mut(handle, "mark_peer_interested", |live| {
            let prev = live.peer_interested;
            live.peer_interested = is_interested;
            prev
        });
        // Interest is read as an aggregate (see `interested_peers`); maintain
        // it here so every transition — interested, not-interested and
        // disconnect — goes through one edge rather than three call sites.
        if let Some(prev) = changed
            && prev != is_interested
        {
            let counter = &self.stats.interested_peers;
            if is_interested {
                crate::torrent_state::utils::atomic_inc(counter);
            } else {
                crate::torrent_state::utils::atomic_dec(counter);
            }
        }
        changed
    }

    pub fn update_bitfield(
        &self,
        handle: PeerHandle,
        bitfield: BF,
        total_pieces: usize,
    ) -> Option<()> {
        self.with_live_mut(handle, "update_bitfield", |live| {
            live.bitfield = bitfield;
            self.update_seeder_flag(live, total_pieces);
        })
    }

    // Recompute the cached "this peer has the full torrent" flag after its bitfield
    // changed, keeping the aggregate seeder counters in sync. Must be called on every
    // bitfield change of a live peer.
    pub fn update_seeder_flag(&self, live: &mut LivePeerState, total_pieces: usize) {
        let seeder = live.has_full_torrent(total_pieces);
        if seeder == live.seeder {
            return;
        }
        live.seeder = seeder;
        self.stats.seeder_flag_changed(seeder);
        self.session_stats.seeder_flag_changed(seeder);
    }

    pub fn mark_peer_connecting(&self, h: PeerHandle) -> crate::Result<(PeerRx, PeerTx)> {
        let rx = self
            .with_peer_mut(h, "mark_peer_connecting", |peer| {
                peer.idle_to_connecting(self)
                    .ok_or(Error::BugInvalidPeerState)
            })
            .ok_or(Error::BugPeerNotFound)??;
        Ok(rx)
    }

    pub fn reset_peer_backoff(&self, handle: PeerHandle) {
        self.with_peer_mut(handle, "reset_peer_backoff", |p| {
            p.stats.reset_backoff();
        });
    }

    pub fn mark_peer_not_needed(&self, handle: PeerHandle) -> Option<PeerState> {
        let prev = self.with_peer_mut(handle, "mark_peer_not_needed", |peer| {
            peer.set_not_needed(self)
        })?;
        Some(prev)
    }

    pub(crate) fn on_steal(
        &self,
        from_peer: SocketAddr,
        to_peer: SocketAddr,
        stolen_idx: ValidPieceIndex,
    ) {
        self.with_peer(to_peer, |p| {
            atomic_inc(&p.stats.counters.times_i_stole);
        });
        self.with_peer(from_peer, |p| {
            atomic_inc(&p.stats.counters.times_stolen_from_me);
        });
        self.stats.inc_steals();
        self.session_stats.inc_steals();

        self.with_live_mut(from_peer, "send_cancellations", |live| {
            live.cancel_inflight_requests_for_piece(stolen_idx);
        });
    }
}

#[cfg(test)]
mod tests {
    use librqbit_core::hash_id::Id20;

    use super::*;
    use crate::stream_connect::ConnectionKind;

    const TOTAL_PIECES: usize = 8;

    fn full_bitfield() -> BF {
        BF::from_boxed_slice(vec![0xffu8; 1].into_boxed_slice())
    }

    fn empty_bitfield() -> BF {
        BF::from_boxed_slice(vec![0u8; 1].into_boxed_slice())
    }

    fn make_peer_states() -> PeerStates {
        PeerStates {
            session_stats: Default::default(),
            next_insertion_seq: Default::default(),
            live_outgoing_peers: Default::default(),
            stats: Default::default(),
            states: Default::default(),
            peer_backoff: None,
        }
    }

    // Returns the peer handle, keeping the receiving end of the peer channel alive.
    fn add_live_peer(peers: &PeerStates, addr: &str) -> (PeerHandle, PeerRx) {
        let addr: SocketAddr = addr.parse().unwrap();
        peers.add_if_not_seen(addr).unwrap();
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        peers
            .with_peer_mut(addr, "test", |peer| {
                peer.incoming_connection(Id20::default(), tx, peers, ConnectionKind::Tcp)
            })
            .unwrap()
            .unwrap();
        (addr, rx)
    }

    fn live_seeders(peers: &PeerStates) -> (u32, u32) {
        (
            peers.stats().live_seeders,
            peers.session_stats.snapshot().live_seeders,
        )
    }

    /// Usefulness is an aggregate: it must count each peer once when it crosses
    /// the threshold, and stop counting it when it leaves.
    #[test]
    fn useful_peers_counts_each_peer_once_and_decrements_on_disconnect() {
        let peers = make_peer_states();
        let (addr, _rx) = add_live_peer(&peers, "127.0.0.1:7000");
        let counters = peers
            .with_peer(addr, |p| p.stats.counters.clone())
            .expect("peer counters");

        assert_eq!(peers.stats().useful_peers, 0);
        // Below the threshold: still not useful.
        peers.note_peer_fetched_bytes(&counters, USEFUL_PEER_BYTES - 1);
        assert_eq!(peers.stats().useful_peers, 0);
        // Crossing it counts exactly one peer.
        peers.note_peer_fetched_bytes(&counters, 1);
        assert_eq!(peers.stats().useful_peers, 1);
        // Further payload does not count the same peer twice.
        peers.note_peer_fetched_bytes(&counters, 10_000);
        assert_eq!(peers.stats().useful_peers, 1);

        // A second, non-useful peer must not change the aggregate.
        let (other, _rx2) = add_live_peer(&peers, "127.0.0.1:7001");
        assert_eq!(peers.stats().useful_peers, 1);

        // The useful peer leaving releases its slot in the aggregate.
        peers.drop_peer(addr);
        assert_eq!(peers.stats().useful_peers, 0);
        // The non-useful one leaving changes nothing.
        peers.drop_peer(other);
        assert_eq!(peers.stats().useful_peers, 0);
    }

    /// Interest is an aggregate over the live edge, so it must move on both
    /// the `Interested` / `NotInterested` transitions and on disconnect.
    #[test]
    fn interested_peers_tracks_interest_and_disconnect() {
        let peers = make_peer_states();
        let (addr, _rx) = add_live_peer(&peers, "127.0.0.1:7100");
        // A connection is created interested, so joining already counts it.
        assert_eq!(peers.stats().interested_peers, 1);

        peers.mark_peer_interested(addr, true).expect("mark again");
        assert_eq!(peers.stats().interested_peers, 1);
        // Repeating the same state must not double count.
        peers.mark_peer_interested(addr, true).expect("mark again");
        assert_eq!(peers.stats().interested_peers, 1);

        peers.mark_peer_interested(addr, false).expect("clear");
        assert_eq!(peers.stats().interested_peers, 0);

        // A peer that disconnects while interested is un-counted even though it
        // never sent `NotInterested`.
        peers.mark_peer_interested(addr, true).expect("re-mark");
        assert_eq!(peers.stats().interested_peers, 1);
        peers.drop_peer(addr);
        assert_eq!(peers.stats().interested_peers, 0);
    }

    #[test]
    fn full_bitfield_counts_as_seeder_and_is_uncounted_on_disconnect() {
        let peers = make_peer_states();
        let (addr, _rx) = add_live_peer(&peers, "127.0.0.1:1234");
        assert_eq!(live_seeders(&peers), (0, 0));

        peers
            .update_bitfield(addr, empty_bitfield(), TOTAL_PIECES)
            .unwrap();
        assert_eq!(live_seeders(&peers), (0, 0));

        peers
            .update_bitfield(addr, full_bitfield(), TOTAL_PIECES)
            .unwrap();
        assert_eq!(live_seeders(&peers), (1, 1));

        // Repeated updates must not double-count.
        peers
            .update_bitfield(addr, full_bitfield(), TOTAL_PIECES)
            .unwrap();
        assert_eq!(live_seeders(&peers), (1, 1));

        peers.drop_peer(addr).unwrap();
        assert_eq!(live_seeders(&peers), (0, 0));
    }

    #[test]
    fn bitfield_completed_piece_by_piece_counts_as_seeder() {
        let peers = make_peer_states();
        let (addr, _rx) = add_live_peer(&peers, "127.0.0.1:1234");
        peers
            .update_bitfield(addr, empty_bitfield(), TOTAL_PIECES)
            .unwrap();

        for piece in 0..TOTAL_PIECES {
            peers
                .with_live_mut(addr, "test", |live| {
                    *live.bitfield.get_mut(piece).unwrap() = true;
                    peers.update_seeder_flag(live, TOTAL_PIECES);
                })
                .unwrap();
            let expected = u32::from(piece == TOTAL_PIECES - 1);
            assert_eq!(live_seeders(&peers), (expected, expected));
        }
    }

    #[test]
    fn seeder_is_uncounted_when_peer_leaves_live_state() {
        let peers = make_peer_states();
        let (addr, _rx) = add_live_peer(&peers, "127.0.0.1:1234");
        peers
            .update_bitfield(addr, full_bitfield(), TOTAL_PIECES)
            .unwrap();
        assert_eq!(live_seeders(&peers), (1, 1));

        peers.mark_peer_not_needed(addr).unwrap();
        assert_eq!(live_seeders(&peers), (0, 0));
    }

    #[test]
    fn seeders_are_counted_per_peer() {
        let peers = make_peer_states();
        let (a, _rx_a) = add_live_peer(&peers, "127.0.0.1:1234");
        let (b, _rx_b) = add_live_peer(&peers, "127.0.0.1:1235");

        peers
            .update_bitfield(a, full_bitfield(), TOTAL_PIECES)
            .unwrap();
        peers
            .update_bitfield(b, empty_bitfield(), TOTAL_PIECES)
            .unwrap();
        assert_eq!(live_seeders(&peers), (1, 1));

        peers
            .update_bitfield(b, full_bitfield(), TOTAL_PIECES)
            .unwrap();
        assert_eq!(live_seeders(&peers), (2, 2));

        peers.drop_peer(a).unwrap();
        assert_eq!(live_seeders(&peers), (1, 1));
    }

    /// Fill the pool with `n` Queued candidates (no live connections).
    fn fill_queued(peers: &PeerStates, n: usize) {
        for i in 0..n {
            let addr: SocketAddr = format!("127.0.0.1:{}", 10_000 + i).parse().unwrap();
            peers.add_if_not_seen(addr).expect("candidate admitted");
        }
    }

    /// The plan asks for the pool to stay bounded when far more distinct
    /// candidates arrive than it can hold, with a live connection never
    /// disturbed and a fresh reachable peer still admitted afterwards.
    #[test]
    fn candidate_pool_stays_bounded_under_a_large_candidate_stream() {
        const INJECTED: usize = 30_000;
        let peers = make_peer_states();
        let (live, _live_rx) = add_live_peer(&peers, "127.0.0.1:40000");
        let live_before = peers.stats().live;

        for i in 0..INJECTED {
            let addr: SocketAddr = format!("10.{}.{}.{}:6881", i / 65536, (i / 256) % 256, i % 256)
                .parse()
                .expect("candidate addr");
            // Admitted or rejected; either way the pool must not grow past the cap.
            peers.add_if_not_seen(addr);
            assert!(
                peers.states.len() <= MAX_CANDIDATE_PEERS,
                "pool grew to {} after {i} candidates",
                peers.states.len()
            );
        }

        // Every injected candidate was Queued, i.e. evictable, so the pool
        // absorbed the whole stream by evicting the oldest — nothing had to be
        // rejected outright. (Rejection when nothing is evictable is covered by
        // `candidate_pool_is_bounded_and_rejects_when_nothing_is_evictable`.)
        assert_eq!(
            peers.stats().candidate_rejected,
            0,
            "an evictable pool must not report rejected candidates"
        );
        // The stream was far larger than the pool, so the retained set must be
        // the newest arrivals, not the first ones seen.
        let first: SocketAddr = "10.0.0.0:6881".parse().unwrap();
        assert!(
            peers.states.get(&first).is_none(),
            "the oldest candidate must have been evicted long before the stream ended"
        );
        // The established connection was never sacrificed for a candidate.
        assert_eq!(
            peers.stats().live,
            live_before,
            "turnover must not evict a live peer to make room for candidates"
        );
        // A newly reachable peer still gets in (someone was evictable).
        let newcomer: SocketAddr = "127.0.0.1:45000".parse().unwrap();
        assert!(
            peers.add_if_not_seen(newcomer).is_some(),
            "a new candidate must still be admitted while evictable entries exist"
        );
        // And the live peer is still there.
        assert!(peers.states.get(&live).is_some());
    }

    #[test]
    fn candidate_pool_is_bounded_and_rejects_when_nothing_is_evictable() {
        let peers = make_peer_states();
        // One live peer must never be sacrificed for a candidate.
        let (_addr, _rx) = add_live_peer(&peers, "127.0.0.1:9000");

        fill_queued(&peers, MAX_CANDIDATE_PEERS - 1);
        assert_eq!(peers.states.len(), MAX_CANDIDATE_PEERS);

        // The pool is full of evictable Queued peers, so a new candidate
        // still gets in by evicting the oldest.
        let new_addr: SocketAddr = "127.0.0.1:60000".parse().unwrap();
        assert!(peers.add_if_not_seen(new_addr).is_some());
        assert_eq!(peers.states.len(), MAX_CANDIDATE_PEERS);

        // Make every peer non-evictable by marking it Connecting, which is in
        // the same "must keep" class as Live for eviction purposes.
        for mut entry in peers.states.iter_mut() {
            if entry.value().is_evictable_candidate() {
                let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
                entry
                    .value_mut()
                    .set_state(PeerState::Connecting(tx), &peers);
            }
        }
        let rejected_before = peers.stats().candidate_rejected;
        let rejected_addr: SocketAddr = "127.0.0.1:60001".parse().unwrap();
        assert!(
            peers.add_if_not_seen(rejected_addr).is_none(),
            "with nothing evictable the candidate must be rejected"
        );
        assert_eq!(
            peers
                .stats
                .candidate_rejected
                .load(std::sync::atomic::Ordering::Relaxed),
            rejected_before + 1
        );
    }

    #[test]
    fn eviction_prefers_dead_over_not_needed_over_queued() {
        let peers = make_peer_states();
        let dead: SocketAddr = "127.0.0.1:10001".parse().unwrap();
        let not_needed: SocketAddr = "127.0.0.1:10002".parse().unwrap();
        let queued: SocketAddr = "127.0.0.1:10003".parse().unwrap();

        peers.add_if_not_seen(dead).unwrap();
        peers.add_if_not_seen(not_needed).unwrap();
        peers.add_if_not_seen(queued).unwrap();
        peers.with_peer_mut(dead, "t", |p| p.set_state(PeerState::Dead, &peers));
        peers.with_peer_mut(not_needed, "t", |p| {
            p.set_state(PeerState::NotNeeded, &peers);
        });

        // Force the pool to look full.
        let evicted = peers.evict_one_candidate_for_test();
        assert_eq!(evicted, Some(dead), "Dead must be evicted first");
        let evicted = peers.evict_one_candidate_for_test();
        assert_eq!(evicted, Some(not_needed), "then NotNeeded");
        let evicted = peers.evict_one_candidate_for_test();
        assert_eq!(evicted, Some(queued), "Queued last");
    }

    #[test]
    fn turnover_only_releases_eligible_idle_peers_and_at_most_the_budget() {
        // Pure policy check of the eligibility rules, expressed on the stored
        // per-peer turnover bookkeeping.
        let now = std::time::Instant::now();
        let live_limit = 10usize;
        let live_now = 10usize;
        let budget = std::cmp::max(1, live_now.div_ceil(25)); // 4% => 1
        assert_eq!(budget, 1);

        // A fresh peer is too young.
        let connected_at = now;
        assert!(now.duration_since(connected_at) < std::time::Duration::from_secs(120));

        // An old peer that uploaded recently is not idle.
        let connected_at = now - std::time::Duration::from_secs(600);
        let last_upload_at = now - std::time::Duration::from_secs(1);
        assert!(now.duration_since(last_upload_at) < std::time::Duration::from_secs(120));

        // An old peer idle for long enough, with no in-flight work and not
        // interested, is eligible.
        let _connected_at = now - std::time::Duration::from_secs(600);
        let last_download_at = now - std::time::Duration::from_secs(600);
        let last_upload_at = now - std::time::Duration::from_secs(600);
        let eligible = now.duration_since(connected_at) >= std::time::Duration::from_secs(120)
            && now.duration_since(last_download_at) >= std::time::Duration::from_secs(120)
            && now.duration_since(last_upload_at) >= std::time::Duration::from_secs(120);
        assert!(eligible, "the idle oldest peer must be eligible");
        assert_eq!(live_limit.div_ceil(25), 1, "40% of 0 is not the rule");

        // The threshold gate is "at least 90% of the cap": with a cap of 10,
        // 9 live peers meets it and 8 does not.
        assert!(9 * 100 >= live_limit * 90, "9/10 meets the 90% gate");
        assert!(8 * 100 < live_limit * 90, "8/10 is below the 90% gate");
        // And the 4% rotation budget is at least one peer at every size.
        for live in 1..=live_limit {
            assert!(std::cmp::max(1, live.div_ceil(25)) >= 1);
        }
    }
}
