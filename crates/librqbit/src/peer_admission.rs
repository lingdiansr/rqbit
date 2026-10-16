//! Session-wide peer admission.
//!
//! Upstream gives each torrent exactly one semaphore that covers the whole
//! peer lifetime: an outgoing connection takes a permit before dialling and
//! only releases it when the connection ends, and an incoming connection that
//! cannot take a permit is dropped on the floor.  On a public swarm most
//! connection attempts never complete, so the budget fills up with peers that
//! are merely *trying*, and established peers starve.
//!
//! This module separates the two:
//!
//! * [`PeerAdmission`] is the session-wide governor.  It owns the logical-peer
//!   budget, the connecting budget, and an outbound-only share of each so that
//!   inbound connections always have reserved headroom.
//! * [`TorrentAdmission`] is the per-torrent budget: an outbound connecting
//!   cap (also the dial source quota) plus the live-peer cap.
//! * [`PeerConnectionLease`] is the RAII handle.  It holds the permits for one
//!   logical peer, releases the connecting permits when the peer is promoted
//!   to live, and releases everything when it is dropped.
//!
//! Every permit acquisition is a synchronous `try_acquire`: a caller never
//! holds partial permits while awaiting another budget, and an acquisition
//! that fails leaves nothing behind.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::bail;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Tunables for session-wide peer admission.
///
/// The defaults are conservative resource budgets, not values measured to be
/// optimal on a public swarm.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PeerAdmissionConfig {
    /// Logical peer connections across the whole session, counting dialling,
    /// handshaking, magnet metadata and live peers.  This is a budget for
    /// *peer connections*, not an operating-system file-descriptor limit.
    pub session_peer_limit: usize,
    /// Outbound *connecting* peers per torrent.
    pub torrent_connect_limit: usize,
    /// Connecting peers across the whole session, including metadata probes
    /// and inbound connections waiting to be checked.
    pub session_connect_limit: usize,
    /// Outbound dials per second across the whole session.
    pub outbound_connect_rate: u32,
}

impl Default for PeerAdmissionConfig {
    fn default() -> Self {
        Self {
            session_peer_limit: 1024,
            torrent_connect_limit: 64,
            session_connect_limit: 128,
            outbound_connect_rate: 30,
        }
    }
}

impl PeerAdmissionConfig {
    /// Reject budgets that would wedge admission at construction time rather
    /// than silently refusing every peer later.
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.session_peer_limit == 0 {
            bail!("peer admission: session_peer_limit must be non-zero");
        }
        if self.torrent_connect_limit == 0 {
            bail!("peer admission: torrent_connect_limit must be non-zero");
        }
        if self.session_connect_limit == 0 {
            bail!("peer admission: session_connect_limit must be non-zero");
        }
        if self.outbound_connect_rate == 0 {
            bail!("peer admission: outbound_connect_rate must be non-zero");
        }
        Ok(())
    }

    /// Headroom kept for inbound connections out of the logical-peer budget.
    pub fn global_reserve(&self) -> usize {
        reserve(self.session_peer_limit, 16, 8, 64)
    }

    /// Headroom kept for inbound connections out of the connecting budget.
    pub fn connecting_reserve(&self) -> usize {
        reserve(self.session_connect_limit, 8, 2, 16)
    }

    /// Upper bound on metadata probes in flight at once across the session.
    ///
    /// A magnet read probes a peer that may never answer; letting a session
    /// open one socket per address it hears about would let a single magnet
    /// exhaust the connecting budget before the first successful read.  The
    /// cap is the per-torrent connecting quota, itself clamped to a small
    /// constant so the bound always derives from configuration.
    pub fn max_concurrent_metadata_reads(&self) -> usize {
        self.torrent_connect_limit
            .min(MAX_CONCURRENT_METADATA_READS)
    }
}

/// Hard ceiling on concurrent metadata probes per session.  Named rather than
/// inlined so the bound in [`PeerAdmissionConfig::max_concurrent_metadata_reads`]
/// reads as configuration, not as a stray literal.
const MAX_CONCURRENT_METADATA_READS: usize = 8;

/// The outbound share of `budget` once `reserve` is set aside for inbound.
///
/// Never returns zero for a non-zero budget: a configuration must always admit
/// at least one outbound peer, even a deliberately tiny test budget.
fn outbound_budget(budget: usize, reserve: usize) -> usize {
    budget.saturating_sub(reserve).max(1).min(budget)
}

/// Keep `budget / divisor` slots, clamped to `[min, max]` and never the whole
/// budget, so at least one slot stays available to the other direction.
fn reserve(budget: usize, divisor: usize, min: usize, max: usize) -> usize {
    let raw = (budget / divisor.max(1)).clamp(min, max);
    raw.min(budget.saturating_sub(1))
}

/// Which budget refused an admission.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionDenied {
    /// The session-wide logical-peer budget is exhausted.
    SessionPeers,
    /// The session-wide connecting budget is exhausted.
    SessionConnecting,
    /// The per-torrent outbound connecting quota is exhausted.
    TorrentConnecting,
    /// The per-torrent live-peer budget is exhausted.
    TorrentLive,
}

impl AdmissionDenied {
    /// Stable label for logs and counters.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::SessionPeers => "session-peer-limit",
            Self::SessionConnecting => "session-connecting-limit",
            Self::TorrentConnecting => "torrent-connecting-limit",
            Self::TorrentLive => "torrent-live-limit",
        }
    }
}

/// Session-wide peer governor.  Cheap to clone behind an `Arc`.
#[derive(Debug)]
pub struct PeerAdmission {
    config: PeerAdmissionConfig,
    global: Arc<Semaphore>,
    global_outbound: Arc<Semaphore>,
    connecting: Arc<Semaphore>,
    connecting_outbound: Arc<Semaphore>,
    rejected: AtomicU64,
}

impl PeerAdmission {
    pub fn new(config: PeerAdmissionConfig) -> anyhow::Result<Self> {
        config.validate()?;
        let global_reserve = config.global_reserve();
        let connecting_reserve = config.connecting_reserve();
        Ok(Self {
            config,
            global: Arc::new(Semaphore::new(config.session_peer_limit)),
            global_outbound: Arc::new(Semaphore::new(outbound_budget(
                config.session_peer_limit,
                global_reserve,
            ))),
            connecting: Arc::new(Semaphore::new(config.session_connect_limit)),
            connecting_outbound: Arc::new(Semaphore::new(outbound_budget(
                config.session_connect_limit,
                connecting_reserve,
            ))),
            rejected: AtomicU64::new(0),
        })
    }

    pub fn config(&self) -> &PeerAdmissionConfig {
        &self.config
    }

    /// How many admissions this governor has refused so far.
    pub fn rejected(&self) -> u64 {
        self.rejected.load(Ordering::Relaxed)
    }

    /// Take the permits for one **outgoing** logical peer.
    ///
    /// Held in this order: session logical budget, the outbound share of it,
    /// session connecting budget, the outbound share of that, and finally the
    /// source's own connecting quota.  Any failure releases everything already
    /// taken.
    pub(crate) fn try_acquire_outgoing(
        &self,
        source: &TorrentAdmission,
    ) -> Result<PeerConnectionLease, AdmissionDenied> {
        let lease = self.acquire(LeaseKind::Outgoing, Some(source))?;
        Ok(lease)
    }

    /// Take the permits for one **incoming** logical peer.
    ///
    /// Incoming connections draw on the reserved headroom: they take the full
    /// logical and connecting budgets but never the outbound shares.
    pub(crate) fn try_acquire_incoming(&self) -> Result<PeerConnectionLease, AdmissionDenied> {
        self.acquire(LeaseKind::Incoming, None)
    }

    /// Take the connecting budget for one **metadata probe**.
    ///
    /// A magnet read has no torrent yet, so it has no per-torrent quota, and
    /// it must not hold a logical-peer slot: probes are short and mostly fail,
    /// and letting them draw on the peer budget would starve the peers that
    /// are actually transferring.  It is outbound, so it takes the outbound
    /// *share* of the connecting budget (never the inbound reserve) and
    /// nothing else.  Returned as a distinct lease type so it is impossible to
    /// promote a metadata probe to a live peer.
    pub(crate) fn try_acquire_metadata(&self) -> Result<MetadataLease, AdmissionDenied> {
        let connecting = self.try_take(
            &self.connecting_outbound,
            AdmissionDenied::SessionConnecting,
        )?;
        Ok(MetadataLease {
            _connecting: connecting,
        })
    }

    fn acquire(
        &self,
        kind: LeaseKind,
        source: Option<&TorrentAdmission>,
    ) -> Result<PeerConnectionLease, AdmissionDenied> {
        let mut lease = PeerConnectionLease::empty(kind);

        // Every failure unwinds by clearing the whole lease, so a refused
        // acquisition never leaves part of the budget taken.  Kept as plain
        // statements rather than a helper closure so each `?` releases the
        // previous borrow before the next field is touched.
        lease.global = Some(self.try_take(&self.global, AdmissionDenied::SessionPeers)?);
        match self.try_take(&self.connecting, AdmissionDenied::SessionConnecting) {
            Ok(permit) => lease.connecting = Some(permit),
            Err(denied) => {
                lease.release_all();
                return Err(denied);
            }
        }

        if kind.is_outbound() {
            match self.try_take(&self.global_outbound, AdmissionDenied::SessionPeers) {
                Ok(permit) => lease.global_outbound = Some(permit),
                Err(denied) => {
                    lease.release_all();
                    return Err(denied);
                }
            }
            match self.try_take(
                &self.connecting_outbound,
                AdmissionDenied::SessionConnecting,
            ) {
                Ok(permit) => lease.connecting_outbound = Some(permit),
                Err(denied) => {
                    lease.release_all();
                    return Err(denied);
                }
            }
            if let Some(source) = source {
                match source.try_acquire_connecting() {
                    Ok(permit) => lease.source = Some(permit),
                    Err(denied) => {
                        lease.release_all();
                        return Err(denied);
                    }
                }
            }
        }

        Ok(lease)
    }

    fn try_take(
        &self,
        semaphore: &Arc<Semaphore>,
        denied: AdmissionDenied,
    ) -> Result<OwnedSemaphorePermit, AdmissionDenied> {
        match Arc::clone(semaphore).try_acquire_owned() {
            Ok(permit) => Ok(permit),
            Err(_) => {
                self.rejected.fetch_add(1, Ordering::Relaxed);
                Err(denied)
            }
        }
    }
}

/// Per-torrent peer budgets.
#[derive(Debug)]
pub(crate) struct TorrentAdmission {
    live_limit: usize,
    connecting: Arc<Semaphore>,
    live: Arc<Semaphore>,
    live_outbound: Arc<Semaphore>,
}

impl TorrentAdmission {
    /// `live_limit` is the per-torrent established-peer cap (the public
    /// `peer_limit` option); `connect_limit` is the per-torrent outbound
    /// connecting cap, shared with the session's dial sources.
    pub(crate) fn new(live_limit: usize, connect_limit: usize) -> Self {
        let live_limit = live_limit.max(1);
        let connect_limit = connect_limit.max(1);
        let live_reserve = reserve(live_limit, 16, 1, 32);
        Self {
            live_limit,
            connecting: Arc::new(Semaphore::new(connect_limit)),
            live: Arc::new(Semaphore::new(live_limit)),
            live_outbound: Arc::new(Semaphore::new(outbound_budget(live_limit, live_reserve))),
        }
    }

    /// The per-torrent established-peer cap (the public `peer_limit`).
    pub(crate) fn live_limit(&self) -> usize {
        self.live_limit
    }

    pub(crate) fn try_acquire_connecting(&self) -> Result<OwnedSemaphorePermit, AdmissionDenied> {
        Arc::clone(&self.connecting)
            .try_acquire_owned()
            .map_err(|_| AdmissionDenied::TorrentConnecting)
    }

    /// Take the per-torrent live permits, respecting the outbound share for
    /// outgoing peers.  A failure rolls back the permits it already took.
    pub(crate) fn try_acquire_live(
        &self,
        kind: LeaseKind,
    ) -> Result<(OwnedSemaphorePermit, Option<OwnedSemaphorePermit>), AdmissionDenied> {
        let live = Arc::clone(&self.live)
            .try_acquire_owned()
            .map_err(|_| AdmissionDenied::TorrentLive)?;
        if !kind.is_outbound() {
            return Ok((live, None));
        }
        match Arc::clone(&self.live_outbound).try_acquire_owned() {
            Ok(outbound) => Ok((live, Some(outbound))),
            Err(_) => {
                // Roll back: dropping `live` here returns it to the semaphore.
                drop(live);
                Err(AdmissionDenied::TorrentLive)
            }
        }
    }
}

/// Direction of the connection a lease was admitted for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LeaseKind {
    Incoming,
    Outgoing,
}

impl LeaseKind {
    pub(crate) fn is_outbound(self) -> bool {
        matches!(self, Self::Outgoing)
    }
}

/// RAII permits for one logical peer connection.
///
/// Dropping the lease releases every permit it still holds, so the error,
/// cancellation and disconnect paths cannot leak capacity.  The connecting
/// permits are additionally released early, when the peer is promoted to live.
#[derive(Debug)]
pub(crate) struct PeerConnectionLease {
    kind: LeaseKind,
    global: Option<OwnedSemaphorePermit>,
    global_outbound: Option<OwnedSemaphorePermit>,
    connecting: Option<OwnedSemaphorePermit>,
    connecting_outbound: Option<OwnedSemaphorePermit>,
    source: Option<OwnedSemaphorePermit>,
    live: Option<OwnedSemaphorePermit>,
    live_outbound: Option<OwnedSemaphorePermit>,
}

impl PeerConnectionLease {
    fn empty(kind: LeaseKind) -> Self {
        Self {
            kind,
            global: None,
            global_outbound: None,
            connecting: None,
            connecting_outbound: None,
            source: None,
            live: None,
            live_outbound: None,
        }
    }

    /// Release everything held.  Used to unwind a partially completed
    /// acquisition; `Drop` is the steady-state path.
    fn release_all(&mut self) {
        self.global = None;
        self.global_outbound = None;
        self.connecting = None;
        self.connecting_outbound = None;
        self.source = None;
        self.live = None;
        self.live_outbound = None;
    }

    /// Whether this peer already holds live capacity.
    pub(crate) fn is_live(&self) -> bool {
        self.live.is_some()
    }

    /// Promote the peer to live, releasing its connecting permits.
    ///
    /// Synchronous by design: it runs from the handshake callback, where no
    /// `.await` is available, and it must never hold the connecting permits
    /// while waiting for live capacity.
    pub(crate) fn try_promote_live(
        &mut self,
        torrent: &TorrentAdmission,
    ) -> Result<(), AdmissionDenied> {
        if self.is_live() {
            return Ok(());
        }
        let (live, live_outbound) = torrent.try_acquire_live(self.kind)?;
        self.live = Some(live);
        self.live_outbound = live_outbound;
        // The connection is established; its connecting quota is free again.
        self.connecting = None;
        self.connecting_outbound = None;
        self.source = None;
        Ok(())
    }
}

/// RAII permit for one magnet metadata probe.
///
/// Deliberately not a [`PeerConnectionLease`]: a probe holds only a share of
/// the session connecting budget, so it can neither consume the logical-peer
/// budget nor be promoted to a live peer.  Dropping it returns the permit.
#[derive(Debug)]
pub(crate) struct MetadataLease {
    _connecting: OwnedSemaphorePermit,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> PeerAdmissionConfig {
        PeerAdmissionConfig {
            session_peer_limit: 64,
            torrent_connect_limit: 8,
            session_connect_limit: 16,
            outbound_connect_rate: 30,
        }
    }

    #[test]
    fn zero_budgets_are_rejected() {
        for mutate in [
            |c: &mut PeerAdmissionConfig| c.session_peer_limit = 0,
            |c: &mut PeerAdmissionConfig| c.torrent_connect_limit = 0,
            |c: &mut PeerAdmissionConfig| c.session_connect_limit = 0,
            |c: &mut PeerAdmissionConfig| c.outbound_connect_rate = 0,
        ] {
            let mut c = config();
            mutate(&mut c);
            assert!(
                PeerAdmission::new(c).is_err(),
                "config {c:?} must be rejected"
            );
        }
        assert!(PeerAdmission::new(config()).is_ok());
    }

    #[test]
    fn reserve_never_consumes_the_whole_budget() {
        assert_eq!(reserve(1, 16, 8, 64), 0);
        assert_eq!(reserve(2, 16, 8, 64), 1);
        assert_eq!(outbound_budget(1, reserve(1, 16, 8, 64)), 1);
    }

    #[test]
    fn outbound_cannot_consume_the_inbound_reserve() {
        let admission = PeerAdmission::new(config()).expect("admission");
        let reserve = admission.config().global_reserve();
        // Generous per-torrent budgets and immediate promotion, so the only
        // budget in play is the session-wide logical-peer one.
        let torrent = TorrentAdmission::new(4096, 4096);

        let mut leases = Vec::new();
        loop {
            let mut lease = match admission.try_acquire_outgoing(&torrent) {
                Ok(lease) => lease,
                Err(_) => break,
            };
            // Promote straight away: releases the connecting permit so the
            // connecting budget cannot become the binding constraint.
            lease.try_promote_live(&torrent).expect("promote");
            leases.push(lease);
        }
        assert_eq!(
            leases.len(),
            admission.config().session_peer_limit - reserve,
            "outbound must stop at the outbound share of the session budget"
        );

        // Inbound still has the reserved headroom.
        let mut inbound = Vec::new();
        for _ in 0..reserve {
            inbound.push(
                admission
                    .try_acquire_incoming()
                    .expect("inbound must use the reserved headroom"),
            );
        }
        assert!(admission.try_acquire_incoming().is_err());

        drop(leases);
        assert!(
            admission.try_acquire_outgoing(&torrent).is_ok(),
            "dropping leases must return capacity"
        );
    }

    #[test]
    fn connecting_budget_binds_before_the_session_peer_budget() {
        // session_connect_limit 16 => outbound connecting share is 14
        // (reserve = clamp(16/8, 2, 16) = 2), well below the 56 logical slots.
        let admission = PeerAdmission::new(config()).expect("admission");
        let torrent = TorrentAdmission::new(4096, 4096);

        let mut leases = Vec::new();
        for _ in 0..14 {
            leases.push(
                admission
                    .try_acquire_outgoing(&torrent)
                    .expect("within the connecting budget"),
            );
        }
        // Not promoting keeps the connecting permits held.
        assert_eq!(
            admission
                .try_acquire_outgoing(&torrent)
                .expect_err("connecting budget exhausted"),
            AdmissionDenied::SessionConnecting
        );

        // Promotion frees connecting capacity for the next dial.
        leases[0].try_promote_live(&torrent).expect("promote");
        assert!(admission.try_acquire_outgoing(&torrent).is_ok());
    }

    #[test]
    fn promotion_releases_connecting_and_applies_the_live_cap() {
        // Large session budgets so the per-torrent live cap is what binds.
        let admission = PeerAdmission::new(PeerAdmissionConfig {
            session_peer_limit: 4096,
            torrent_connect_limit: 4096,
            session_connect_limit: 4096,
            outbound_connect_rate: 30,
        })
        .expect("admission");
        let live_limit = 64;
        let torrent = TorrentAdmission::new(live_limit, 4096);
        let outbound_live_budget = outbound_budget(live_limit, reserve(live_limit, 16, 1, 32));

        let mut first = admission.try_acquire_outgoing(&torrent).expect("first");
        let connecting_before = admission.connecting.available_permits();
        first.try_promote_live(&torrent).expect("promote");
        assert!(first.is_live());
        assert_eq!(
            admission.connecting.available_permits(),
            connecting_before + 1,
            "promotion must return the connecting permit"
        );
        // Promoting twice is a no-op, not a double-take.
        first.try_promote_live(&torrent).expect("idempotent");

        // Fill every remaining outbound live slot.
        let mut leases = vec![first];
        for _ in 1..outbound_live_budget {
            let mut lease = admission.try_acquire_outgoing(&torrent).expect("lease");
            lease.try_promote_live(&torrent).expect("promote");
            leases.push(lease);
        }

        // The next *outbound* peer hits the outbound share of the live budget
        // even though the inbound share is still free.
        let mut overflow = admission.try_acquire_outgoing(&torrent).expect("overflow");
        assert_eq!(
            overflow.try_promote_live(&torrent),
            Err(AdmissionDenied::TorrentLive)
        );

        // Inbound peers still get the reserved share.
        assert!(admission.try_acquire_incoming().is_ok());

        leases.pop();
        assert!(overflow.try_promote_live(&torrent).is_ok());
    }

    #[test]
    fn failed_acquisition_leaves_no_permits_behind() {
        let admission = PeerAdmission::new(config()).expect("admission");
        // One connecting slot only: a second source cannot even start.
        let torrent = TorrentAdmission::new(8, 1);

        let first = admission.try_acquire_outgoing(&torrent).expect("first");
        let err = admission
            .try_acquire_outgoing(&torrent)
            .expect_err("source quota must deny");
        assert_eq!(err, AdmissionDenied::TorrentConnecting);

        // The failed attempt must not have leaked session capacity: with the
        // first lease dropped, the same source can dial again.
        drop(first);
        assert!(admission.try_acquire_outgoing(&torrent).is_ok());
    }

    #[test]
    fn denial_kinds_are_distinguished() {
        let mut cfg = config();
        cfg.session_connect_limit = 1;
        cfg.session_peer_limit = 2;
        let admission = PeerAdmission::new(cfg).expect("admission");

        let _a = admission.try_acquire_incoming().expect("first incoming");
        assert_eq!(
            admission.try_acquire_incoming().expect_err("second"),
            AdmissionDenied::SessionConnecting
        );
        assert!(admission.rejected() >= 1);
    }

    #[test]
    fn metadata_probes_use_the_connecting_budget_only() {
        let mut cfg = config();
        cfg.session_connect_limit = 4;
        cfg.session_peer_limit = 64;
        let admission = PeerAdmission::new(cfg).expect("admission");

        // The outbound connecting share of 4 is reserve = clamp(4/8,2,16)=2,
        // so 2 probes fit and the third is refused.
        let a = admission.try_acquire_metadata().expect("first probe");
        let b = admission.try_acquire_metadata().expect("second probe");
        assert_eq!(
            admission.try_acquire_metadata().expect_err("third probe"),
            AdmissionDenied::SessionConnecting
        );

        // The logical-peer budget is untouched, so incoming peers still fit.
        assert!(admission.try_acquire_incoming().is_ok());

        // Dropping the probes returns exactly the connecting capacity.
        drop(a);
        let _c = admission.try_acquire_metadata().expect("capacity returned");
        drop(b);
    }

    #[test]
    fn metadata_bound_derives_from_config() {
        let cfg = PeerAdmissionConfig {
            torrent_connect_limit: 3,
            ..PeerAdmissionConfig::default()
        };
        assert_eq!(cfg.max_concurrent_metadata_reads(), 3);

        let cfg = PeerAdmissionConfig {
            torrent_connect_limit: 1024,
            ..PeerAdmissionConfig::default()
        };
        assert_eq!(
            cfg.max_concurrent_metadata_reads(),
            MAX_CONCURRENT_METADATA_READS
        );
    }
}

// ---------------------------------------------------------------------------
// Session-wide fair dial scheduler
// ---------------------------------------------------------------------------

/// One torrent's contribution to the session dial scheduler.
///
/// A source owns a queue of candidate addresses and knows how to start a dial
/// for one of them.  The scheduler calls into it; the source never blocks the
/// scheduler waiting for capacity.
pub(crate) trait DialSource: Send + Sync {
    /// The per-torrent budgets this source's candidates must fit into.
    fn admission(&self) -> Arc<TorrentAdmission>;

    /// Peek at the next candidate address, if any is ready right now.
    ///
    /// Must not remove the candidate: the scheduler consumes it with
    /// [`DialSource::commit_candidate`] only once admission succeeded, so a
    /// candidate refused by an exhausted budget is retried on a later round
    /// instead of being lost.
    ///
    /// A `&self` method returning an owned address, so a source has to park
    /// the peeked address (see the trait's implementors) or keep the peek
    /// state internally; it is still only ever called by the scheduler.
    fn try_take_candidate(&self) -> Option<std::net::SocketAddr>;

    /// Consume the candidate last returned by [`DialSource::try_take_candidate`].
    ///
    /// Only the scheduler calls this, only after it acquired the lease, and
    /// only from its single task, so the peek/commit pair cannot race.
    fn commit_candidate(&self);

    /// Whether this source may start a dial now.
    ///
    /// Lets a source keep its own pace (the torrent-level `connect_rate`
    /// option); the default source is always ready.  The scheduler skips a
    /// source that is not due without consuming its candidate.
    fn dial_is_due(&self) -> bool {
        true
    }

    /// Start a dial that already holds its admission lease.
    ///
    /// Takes the source by `Arc` because starting a dial means spawning a
    /// task that outlives the call.
    fn start_dial(self: Arc<Self>, addr: std::net::SocketAddr, lease: PeerConnectionLease);

    /// Opportunity to free slots for waiting candidates (conservative peer
    /// turnover).  Called when a candidate is queued but admission was
    /// refused.  Returns how many peers were released; the default does
    /// nothing, which is the right behaviour for a source with no live peers.
    fn release_idle_peers(&self) -> usize {
        0
    }
}

/// Session-wide fair dialing.
///
/// Every torrent registers as a source.  The scheduler rotates over the
/// sources, starting at most one dial per step, so one torrent with a long
/// backlog cannot starve the others the way per-torrent
/// `semaphore.acquire().await` loops do.  It also enforces the session-wide
/// dial rate and both session budgets.
pub(crate) struct DialScheduler {
    admission: Arc<PeerAdmission>,
    /// Held weakly: the scheduler must not keep a paused or removed torrent
    /// alive.  A dead source is pruned the next time the list is touched, so
    /// "registration" ends by itself when the live state is dropped (pause or
    /// removal) and there is nothing to call at teardown time.
    sources: std::sync::Mutex<Vec<std::sync::Weak<dyn DialSource>>>,
    next_source: std::sync::atomic::AtomicUsize,
    wakeup: tokio::sync::Notify,
}

impl std::fmt::Debug for DialScheduler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DialScheduler")
            .field("admission", &self.admission)
            .field(
                "sources",
                &self.sources.lock().map(|s| s.len()).unwrap_or_default(),
            )
            .finish_non_exhaustive()
    }
}

impl DialScheduler {
    pub(crate) fn new(admission: Arc<PeerAdmission>) -> Self {
        Self {
            admission,
            sources: std::sync::Mutex::new(Vec::new()),
            next_source: std::sync::atomic::AtomicUsize::new(0),
            wakeup: tokio::sync::Notify::new(),
        }
    }

    /// Register a torrent as a dial source.
    ///
    /// The registration is weak: the scheduler keeps this torrent alive only
    /// for the duration of this call, so the caller must hold the source (the
    /// live torrent holds itself) for as long as it should be dialled.
    pub(crate) fn register(&self, source: Arc<dyn DialSource>) {
        let mut sources = self
            .sources
            .lock()
            .expect("dial scheduler source list poisoned");
        // Opportunistic cleanup: dropped torrents must not accumulate here.
        sources.retain(|s| s.strong_count() > 0);
        sources.push(Arc::downgrade(&source));
        drop(sources);
        self.wakeup.notify_one();
    }

    /// Nudge the scheduler after a source gained a candidate.
    pub(crate) fn wake(&self) {
        self.wakeup.notify_one();
    }

    /// Live sources, dropping the entries whose torrent is gone.
    fn sources_snapshot(&self) -> Vec<Arc<dyn DialSource>> {
        let mut sources = self
            .sources
            .lock()
            .expect("dial scheduler source list poisoned");
        let mut alive = Vec::with_capacity(sources.len());
        sources.retain(|s| match s.upgrade() {
            Some(source) => {
                alive.push(source);
                true
            }
            None => false,
        });
        alive
    }

    /// One scheduling round.
    ///
    /// Rotates from where the last round stopped and starts at most one dial,
    /// so the first source cannot monopolise capacity.  Returns `true` if a
    /// dial was started.
    pub(crate) fn step(&self) -> bool {
        let sources = self.sources_snapshot();
        if sources.is_empty() {
            return false;
        }
        let len = sources.len();
        let start = self.next_source.fetch_add(1, Ordering::Relaxed) % len;
        for offset in 0..len {
            let source = &sources[(start + offset) % len];
            let Some(addr) = source.try_take_candidate() else {
                continue;
            };
            if !source.dial_is_due() {
                // This torrent paces its own dials and is not due yet; leave
                // its candidate alone and try the next source.
                continue;
            }
            let torrent = source.admission();
            match self.admission.try_acquire_outgoing(&torrent) {
                Ok(lease) => {
                    // Only now is the candidate really consumed; a refused
                    // source keeps it for a later round.
                    source.commit_candidate();
                    Arc::clone(source).start_dial(addr, lease);
                    return true;
                }
                Err(_) => {
                    // Every peer of this torrent is refusing admission right
                    // now.  Give the source a chance to rotate provably idle
                    // peers out so the queued candidate gets a slot, then try
                    // the next source.  The candidate itself stays queued.
                    source.release_idle_peers();
                    continue;
                }
            }
        }
        false
    }

    /// Run the scheduler forever, paced by `outbound_connect_rate`.
    pub(crate) async fn run(self: Arc<Self>, cancel: tokio_util::sync::CancellationToken) {
        let rate = self.admission.config().outbound_connect_rate.max(1);
        let interval = std::time::Duration::from_micros(1_000_000 / u64::from(rate));
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = ticker.tick() => {}
                _ = self.wakeup.notified() => {}
            }
            loop {
                // Drain what capacity allows on this tick.
                if !self.step() {
                    break;
                }
            }
        }
    }
}

#[cfg(test)]
mod scheduler_tests {
    use super::*;
    use std::net::SocketAddr;
    use std::sync::Mutex;

    #[derive(Default)]
    struct Started {
        dials: Mutex<Vec<(usize, SocketAddr)>>,
    }

    struct TestSource {
        id: usize,
        torrent: Arc<TorrentAdmission>,
        queue: Mutex<Vec<SocketAddr>>,
        started: Arc<Started>,
    }

    impl DialSource for TestSource {
        fn admission(&self) -> Arc<TorrentAdmission> {
            self.torrent.clone()
        }
        fn try_take_candidate(&self) -> Option<SocketAddr> {
            self.queue.lock().expect("queue").first().copied()
        }
        fn commit_candidate(&self) {
            let mut q = self.queue.lock().expect("queue");
            if !q.is_empty() {
                q.remove(0);
            }
        }
        fn start_dial(self: Arc<Self>, addr: SocketAddr, lease: PeerConnectionLease) {
            self.started
                .dials
                .lock()
                .expect("started")
                .push((self.id, addr));
            // Keep the lease alive for the test's duration.
            std::mem::forget(lease);
        }
    }

    fn addr(n: u16) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], n))
    }

    fn source(
        id: usize,
        torrent: Arc<TorrentAdmission>,
        n: u16,
        started: &Arc<Started>,
    ) -> Arc<TestSource> {
        let queue = (0..n)
            .map(|i| addr(1000 + u16::try_from(id).expect("small id") * 100 + i))
            .collect();
        Arc::new(TestSource {
            id,
            torrent,
            queue: Mutex::new(queue),
            started: started.clone(),
        })
    }

    #[test]
    fn dials_rotate_fairly_across_sources() {
        let admission =
            Arc::new(PeerAdmission::new(PeerAdmissionConfig::default()).expect("admission"));
        let scheduler = DialScheduler::new(admission);
        let started = Arc::new(Started::default());

        // One torrent with a huge backlog, one with a single candidate.
        let big = source(0, Arc::new(TorrentAdmission::new(256, 256)), 100, &started);
        let small = source(1, Arc::new(TorrentAdmission::new(256, 256)), 2, &started);
        // The trait object must be the same Arc the scheduler stored.
        let big_dyn: Arc<dyn DialSource> = big.clone();
        let small_dyn: Arc<dyn DialSource> = small.clone();
        scheduler.register(big_dyn);
        scheduler.register(small_dyn);

        // 6 rounds: fairness means the small source is served early rather
        // than only after the big source's 100 candidates are exhausted.
        for _ in 0..6 {
            scheduler.step();
        }
        let dials = started.dials.lock().expect("started").clone();
        let small_hits = dials.iter().filter(|(id, _)| *id == 1).count();
        assert!(
            small_hits >= 2,
            "small source must be served by rotation, got {dials:?}"
        );
    }

    #[test]
    fn exhausted_source_does_not_block_others() {
        let admission =
            Arc::new(PeerAdmission::new(PeerAdmissionConfig::default()).expect("admission"));
        let scheduler = DialScheduler::new(admission);
        let started = Arc::new(Started::default());

        let empty = source(0, Arc::new(TorrentAdmission::new(64, 64)), 0, &started);
        let live = source(1, Arc::new(TorrentAdmission::new(64, 64)), 3, &started);
        // The scheduler holds sources weakly, so the test must keep them.
        let empty_dyn: Arc<dyn DialSource> = empty;
        let live_dyn: Arc<dyn DialSource> = live;
        scheduler.register(empty_dyn.clone());
        scheduler.register(live_dyn.clone());

        for _ in 0..3 {
            assert!(
                scheduler.step(),
                "the non-empty source must still be dialled"
            );
        }
        assert_eq!(started.dials.lock().expect("started").len(), 3);
    }

    #[test]
    fn denied_torrent_does_not_consume_another_torrents_candidates() {
        // Live cap of 1 leaves the outbound live share at 1, so the capped
        // torrent is refused while the other keeps dialling.
        let admission =
            Arc::new(PeerAdmission::new(PeerAdmissionConfig::default()).expect("admission"));
        let scheduler = DialScheduler::new(admission.clone());
        let started = Arc::new(Started::default());

        let capped = source(0, Arc::new(TorrentAdmission::new(1, 1)), 10, &started);
        let healthy = source(1, Arc::new(TorrentAdmission::new(64, 64)), 10, &started);
        let capped_dyn: Arc<dyn DialSource> = capped;
        let healthy_dyn: Arc<dyn DialSource> = healthy;
        scheduler.register(capped_dyn.clone());
        scheduler.register(healthy_dyn.clone());

        for _ in 0..10 {
            scheduler.step();
        }
        let dials = started.dials.lock().expect("started").clone();
        // Each `start_dial` mem::forgets its lease, so after the capped
        // torrent's single live slot is taken it must be refused; the healthy
        // torrent must keep making progress.
        let healthy_hits = dials.iter().filter(|(id, _)| *id == 1).count();
        assert!(
            healthy_hits >= 5,
            "healthy source must keep dialling: {dials:?}"
        );
    }

    #[test]
    fn refused_candidate_is_not_lost() {
        let admission =
            Arc::new(PeerAdmission::new(PeerAdmissionConfig::default()).expect("admission"));
        let scheduler = DialScheduler::new(admission.clone());
        let started = Arc::new(Started::default());

        // A per-torrent connecting quota of one, exhausted by an unrelated
        // (unpromoted) lease, is what refuses the dial here.
        let torrent = Arc::new(TorrentAdmission::new(8, 1));
        let source = source(0, torrent.clone(), 3, &started);
        let blocking = admission
            .try_acquire_outgoing(&torrent)
            .expect("first outgoing lease");
        scheduler.register(source.clone() as Arc<dyn DialSource>);

        // Refused: the candidate must stay queued.
        assert!(!scheduler.step(), "saturated torrent must not be dialled");
        assert!(
            started.dials.lock().expect("started").is_empty(),
            "no dial may have started"
        );
        assert_eq!(
            source.try_take_candidate(),
            Some(addr(1000)),
            "the refused candidate must still be the queue front"
        );

        // Capacity returns: the very same candidate is dialled, not a later one.
        drop(blocking);
        assert!(scheduler.step(), "dial must resume once capacity returns");
        let dials = started.dials.lock().expect("started").clone();
        assert_eq!(dials.len(), 1, "exactly one dial: {dials:?}");
        assert_eq!(
            dials[0].1,
            addr(1000),
            "the refused candidate itself is dialled"
        );
        assert_eq!(
            source.try_take_candidate(),
            Some(addr(1001)),
            "the committed candidate must have left the queue"
        );
    }
}
