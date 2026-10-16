use std::sync::atomic::AtomicU32;

use crate::{
    stream_connect::ConnectionKind,
    torrent_state::{
        live::peer::PeerState,
        peer::LivePeerState,
        utils::{atomic_dec, atomic_inc},
    },
};

gen_stats!(AggregatePeerStatsAtomic AggregatePeerStats, [
    queued u32,
    connecting u32,
    live u32,
    live_tcp u32,
    live_utp u32,
    live_socks u32,
    live_seeders u32,
    seen u32,
    dead u32,
    not_needed u32,
    steals u32,
    candidate_rejected u32,
    // Peers that sent `Interested`: real demand for our data. Maintained by
    // the Interested/NotInterested edges and by disconnect, so it can be read
    // without walking per-peer state.
    interested_peers u32,
    // Peers that have taken at least `peer::stats::atomic::USEFUL_PEER_BYTES`
    // from us. Incremented when a peer first crosses the threshold and
    // decremented when such a peer disconnects, so a reader never has to walk
    // and sum per-peer counters.
    useful_peers u32,
    // Inbound connections turned away before any BT handshake. Session-scoped:
    // the session-level aggregate is the meaningful one; per-torrent readers
    // see the counters of the session that recorded them.
    incoming_rejected u32,
    // Connections refused by the session admission budgets (session-scoped,
    // like `incoming_rejected`).
    admission_rejected u32
], []);

impl AggregatePeerStatsAtomic {
    fn counter(&self, state: &PeerState) -> &AtomicU32 {
        match state {
            PeerState::Connecting(_) => &self.connecting,
            PeerState::Live(_) => &self.live,
            PeerState::Queued => &self.queued,
            PeerState::Dead => &self.dead,
            PeerState::NotNeeded => &self.not_needed,
        }
    }

    fn live_kind_counter(&self, l: &LivePeerState) -> &AtomicU32 {
        match l.connection_kind {
            ConnectionKind::Tcp => &self.live_tcp,
            ConnectionKind::Utp => &self.live_utp,
            ConnectionKind::Socks => &self.live_socks,
        }
    }

    pub(crate) fn inc(&self, state: &PeerState) {
        if let PeerState::Live(l) = state {
            atomic_inc(self.live_kind_counter(l));
            if l.seeder {
                atomic_inc(&self.live_seeders);
            }
            // A connection is created interested (see `LivePeerState::new`), so
            // interest must be maintained here as well as on the protocol
            // edges — otherwise every new peer would be undercounted until it
            // re-sent `Interested`.
            if l.peer_interested {
                atomic_inc(&self.interested_peers);
            }
        }
        atomic_inc(self.counter(state));
    }

    pub(crate) fn dec(&self, state: &PeerState) {
        if let PeerState::Live(l) = state {
            atomic_dec(self.live_kind_counter(l));
            if l.seeder {
                atomic_dec(&self.live_seeders);
            }
            if l.peer_interested {
                atomic_dec(&self.interested_peers);
            }
        }
        atomic_dec(self.counter(state));
    }

    pub(crate) fn seeder_flag_changed(&self, seeder: bool) {
        if seeder {
            atomic_inc(&self.live_seeders);
        } else {
            atomic_dec(&self.live_seeders);
        }
    }

    pub(crate) fn incdec(&self, old: &PeerState, new: &PeerState) {
        self.dec(old);
        self.inc(new);
    }

    pub fn inc_steals(&self) {
        atomic_inc(&self.steals);
    }
}
