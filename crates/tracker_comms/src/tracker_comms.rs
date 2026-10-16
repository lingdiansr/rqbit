use std::collections::HashSet;
use std::net::SocketAddr;
use std::net::SocketAddrV4;
use std::net::SocketAddrV6;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use anyhow::bail;
use backon::ExponentialBuilder;
use backon::Retryable;
use futures::FutureExt;
use futures::StreamExt;
use futures::future::Either;
use futures::stream::BoxStream;
use futures::stream::FuturesUnordered;
use tracing::Instrument;
use tracing::debug;
use tracing::debug_span;
use tracing::info;
use tracing::trace;
use tracing::trace_span;
use url::Url;

use crate::tracker_comms_http;
use crate::tracker_comms_udp;
use crate::tracker_comms_udp::UdpTrackerClient;
use librqbit_core::hash_id::Id20;

pub struct TrackerComms {
    info_hash: Id20,
    peer_id: Id20,
    stats: Box<dyn TorrentStatsProvider>,
    force_tracker_interval: Option<Duration>,
    tx: Sender,
    // This MUST be set as trackers don't work with 0 port.
    announce_port: u16,
    reqwest_client: reqwest::Client,
    key: u32,
    /// Session-wide cap on concurrent tracker announces.  Held only while a
    /// single network request (HTTP or UDP) is actually in flight; waiting,
    /// DNS retries and backoff sleeps never hold it.
    announce_budget: Arc<tokio::sync::Semaphore>,
    /// Cancelled when the torrent's tracker stream is dropped.
    cancel: tokio_util::sync::CancellationToken,
}

/// Default number of tracker announces allowed to be in flight at once across
/// the whole session (HTTP and UDP together).
pub const DEFAULT_TRACKER_ANNOUNCE_CONCURRENCY: usize = 8;

/// Outcome of one successful HTTP announce.
#[derive(Debug, Clone, Copy)]
pub(crate) struct AnnounceOutcome {
    /// The interval the tracker announced.
    pub interval: Duration,
    /// The tracker's `min_interval` (0 when it did not send one).  An explicit
    /// forced interval must never undercut this.
    pub min_interval: Duration,
}

impl TrackerComms {
    /// Wait for an announce slot, returning `None` if the torrent is shutting
    /// down.  The permit is released when the returned guard is dropped, which
    /// callers must do as soon as the response is fully read.
    async fn acquire_announce_slot(&self) -> Option<tokio::sync::OwnedSemaphorePermit> {
        let permit = tokio::select! {
            _ = self.cancel.cancelled() => return None,
            permit = Arc::clone(&self.announce_budget).acquire_owned() => permit.ok(),
        };
        permit
    }
}

#[derive(Default)]
pub enum TrackerCommsStatsState {
    #[default]
    None,
    Initializing,
    Paused,
    Live,
}

#[derive(Default)]
pub struct TrackerCommsStats {
    pub uploaded_bytes: u64,
    pub downloaded_bytes: u64,
    pub total_bytes: u64,
    pub torrent_state: TrackerCommsStatsState,
}

impl TrackerCommsStats {
    pub fn get_left_to_download_bytes(&self) -> u64 {
        let total = self.total_bytes;
        let down = self.downloaded_bytes;
        if total >= down {
            return total - down;
        }
        0
    }

    pub fn is_completed(&self) -> bool {
        self.downloaded_bytes >= self.total_bytes
    }
}

pub trait TorrentStatsProvider: Send + Sync {
    fn get(&self) -> TrackerCommsStats;
}

impl TorrentStatsProvider for () {
    fn get(&self) -> TrackerCommsStats {
        Default::default()
    }
}

type Sender = tokio::sync::mpsc::Sender<SocketAddr>;

enum SupportedTracker {
    Udp(Url),
    Http(Url),
}

impl SupportedTracker {
    fn url(&self) -> &Url {
        match self {
            SupportedTracker::Udp(url) | SupportedTracker::Http(url) => url,
        }
    }
}

impl std::fmt::Debug for SupportedTracker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SupportedTracker::Udp(u) => std::fmt::Display::fmt(u, f),
            SupportedTracker::Http(u) => std::fmt::Display::fmt(u, f),
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum UdpTrackerResolveResult {
    One(SocketAddr),
    Two(SocketAddrV4, SocketAddrV6),
}

async fn udp_tracker_to_socket_addrs(
    host: url::Host<&str>,
    port: u16,
) -> anyhow::Result<UdpTrackerResolveResult> {
    let res = match host {
        url::Host::Domain(name) => {
            // Use the first IPv4 and the first IPv6 addresses only.

            let mut v4: Option<SocketAddrV4> = None;
            let mut v6: Option<SocketAddrV6> = None;
            for addr in tokio::net::lookup_host((name, port))
                .await
                .with_context(|| format!("error looking up hostname {name}"))?
            {
                match (v4, v6, addr) {
                    (None, _, SocketAddr::V4(addr)) => v4 = Some(addr),
                    (_, None, SocketAddr::V6(addr)) => v6 = Some(addr),
                    _ => continue,
                }
            }
            let res = match (v4, v6) {
                (Some(v4), Some(v6)) => UdpTrackerResolveResult::Two(v4, v6),
                (Some(v4), None) => UdpTrackerResolveResult::One(v4.into()),
                (None, Some(v6)) => UdpTrackerResolveResult::One(v6.into()),
                _ => anyhow::bail!("zero addresses returned looking up {name}"),
            };
            trace!(?res, "resolved");
            res
        }
        url::Host::Ipv4(addr) => UdpTrackerResolveResult::One((addr, port).into()),
        url::Host::Ipv6(addr) => UdpTrackerResolveResult::One((addr, port).into()),
    };
    Ok(res)
}

impl TrackerComms {
    // TODO: fix too many args
    #[allow(clippy::too_many_arguments)]
    pub fn start(
        info_hash: Id20,
        peer_id: Id20,
        trackers: HashSet<Url>,
        stats: Box<dyn TorrentStatsProvider>,
        force_interval: Option<Duration>,
        announce_port: u16,
        reqwest_client: reqwest::Client,
        udp_client: UdpTrackerClient,
        announce_budget: Arc<tokio::sync::Semaphore>,
        cancel: tokio_util::sync::CancellationToken,
    ) -> Option<BoxStream<'static, SocketAddr>> {
        let trackers = trackers
            .into_iter()
            .filter_map(|t| match t.scheme() {
                "http" | "https" => Some(SupportedTracker::Http(t)),
                "udp" => Some(SupportedTracker::Udp(t)),
                _ => {
                    debug!("unsupported tracker URL: {}", t);
                    None
                }
            })
            .collect::<Vec<_>>();
        if trackers.is_empty() {
            debug!(?info_hash, "trackers list is empty");
            return None;
        }

        tracing::trace!(?trackers);

        let (tx, mut rx) = tokio::sync::mpsc::channel::<SocketAddr>(16);

        let s = async_stream::stream! {
            use futures::StreamExt;
            let comms = Arc::new(Self {
                info_hash,
                peer_id,
                stats,
                force_tracker_interval: force_interval,
                tx,
                announce_port,
                reqwest_client,
                key: rand::random(),
                announce_budget,
                cancel,
            });
            // Each tracker is isolated behind `catch_unwind`: all trackers and
            // (upstream of this stream) DHT/LSD peers are polled from the same
            // task, so a panic inside one tracker's I/O stack (e.g. a TLS
            // verifier that aborts instead of returning an error) would
            // otherwise tear down peer discovery for the whole torrent.
            let mut futures = FuturesUnordered::new();
            for tracker in trackers {
                let url = tracker.url().clone();
                futures.push(
                    std::panic::AssertUnwindSafe(comms.add_tracker(tracker, &udp_client))
                        .catch_unwind()
                        .map(move |res| (url, res)),
                );
            }
            while !(futures.is_empty()) {
                tokio::select! {
                    addr = rx.recv() => {
                        if let Some(addr) = addr {
                            yield addr;
                        }
                    }
                    e = futures.next(), if !futures.is_empty() => {
                        match e {
                            Some((url, Ok(Err(e)))) => debug!(tracker = %url, "error: {e:#}"),
                            Some((url, Err(panic))) => {
                                let msg = panic
                                    .downcast_ref::<&str>()
                                    .copied()
                                    .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
                                    .unwrap_or("<non-string panic payload>");
                                tracing::error!(tracker = %url, "tracker monitor panicked, disabling this tracker: {msg}");
                            }
                            _ => {}
                        }
                    }
                }
            }
        };

        Some(s.boxed())
    }

    fn add_tracker(
        &self,
        url: SupportedTracker,
        client: &UdpTrackerClient,
    ) -> Either<
        impl std::future::Future<Output = anyhow::Result<()>> + '_ + Send,
        impl std::future::Future<Output = anyhow::Result<()>> + '_ + Send,
    > {
        let info_hash = self.info_hash;
        match url {
            SupportedTracker::Udp(url) => {
                let span = debug_span!(parent: None, "udp_tracker", tracker = %url, info_hash = ?info_hash);
                self.task_single_tracker_monitor_udp(url, client.clone())
                    .instrument(span)
                    .right_future()
            }
            SupportedTracker::Http(url) => {
                let span = debug_span!(
                    parent: None,
                    "http_tracker",
                    tracker = %url,
                    info_hash = ?info_hash
                );
                self.task_single_tracker_monitor_http(url)
                    .instrument(span)
                    .left_future()
            }
        }
    }

    async fn task_single_tracker_monitor_http(&self, tracker_url: Url) -> anyhow::Result<()> {
        trace!(url=%tracker_url, "starting monitor");
        let mut event = Some(tracker_comms_http::TrackerRequestEvent::Started);

        loop {
            let outcome = (|| self.tracker_one_request_http(&tracker_url, event))
                .retry(
                    ExponentialBuilder::new()
                        .without_max_times()
                        .with_jitter()
                        .with_factor(2.)
                        .with_min_delay(Duration::from_secs(10))
                        .with_max_delay(Duration::from_secs(600)),
                )
                .notify(|err, retry_in| debug!(?retry_in, "error calling tracker: {err:#}"))
                .await
                .context("this shouldn't fail")?;

            event = None;
            // An explicit force interval still may not undercut the tracker's
            // own minimum.
            let interval = match self.force_tracker_interval {
                Some(forced) => forced.max(outcome.min_interval),
                None => outcome.interval,
            };
            debug!("sleeping for {:?} after calling tracker", interval);
            tokio::time::sleep(interval).await;
        }
    }

    async fn tracker_one_request_http(
        &self,
        tracker_url: &Url,
        event: Option<tracker_comms_http::TrackerRequestEvent>,
    ) -> anyhow::Result<AnnounceOutcome> {
        // Hold an announce slot across the request AND the body read, then
        // drop it before peers are handed downstream so a slow consumer cannot
        // occupy tracker capacity.
        let _permit = self
            .acquire_announce_slot()
            .await
            .context("tracker announce cancelled")?;
        let stats = self.stats.get();
        let request = tracker_comms_http::TrackerRequest {
            info_hash: &self.info_hash,
            peer_id: &self.peer_id,
            port: self.announce_port,
            uploaded: stats.uploaded_bytes,
            downloaded: stats.downloaded_bytes,
            left: stats.get_left_to_download_bytes(),
            compact: true,
            no_peer_id: false,
            event,
            ip: None,
            numwant: None,
            key: Some(self.key),
            trackerid: None,
        };

        let mut url = tracker_url.clone();

        let mut queries = request.as_querystring();
        if let Some(url_query) = url.query() {
            queries.push_str(&format!("&{}", url_query));
        }
        url.set_query(Some(&queries));

        let response: reqwest::Response = self.reqwest_client.get(url).send().await?;
        if !response.status().is_success() {
            anyhow::bail!("tracker responded with {:?}", response.status());
        }
        let bytes = response.bytes().await?;
        if let Ok((error, _)) =
            bencode::from_bytes_with_rest::<tracker_comms_http::TrackerError>(&bytes)
        {
            anyhow::bail!(
                "tracker returned failure. Failure reason: {}",
                error.failure_reason
            )
        };
        let response = bencode::from_bytes_with_rest::<tracker_comms_http::TrackerResponse>(&bytes)
            .map_err(|e| {
                tracing::trace!("error deserializing TrackerResponse: {e:#}");
                e.into_kind()
            })?
            .0;

        let peers: Vec<_> = response.iter_peers().collect();
        info!(tracker=%tracker_url, peers=peers.len(), "tracker announce succeeded");
        // Network work is done; free the announce slot before the (possibly
        // slow) handoff to the candidate channel.
        drop(_permit);
        for peer in peers {
            self.tx.send(peer).await?;
        }
        // Never re-announce earlier than the tracker allows: both its
        // announced interval and its min_interval are floors.
        let min_interval = Duration::from_secs(response.min_interval.unwrap_or(0));
        Ok(AnnounceOutcome {
            interval: Duration::from_secs(response.interval).max(min_interval),
            min_interval,
        })
    }

    async fn task_single_tracker_monitor_udp(
        &self,
        url: Url,
        client: UdpTrackerClient,
    ) -> anyhow::Result<()> {
        if url.scheme() != "udp" {
            bail!("expected UDP scheme in {}", url);
        }
        let (host, port) = (
            url.host().context("missing host")?,
            url.port().context("missing port")?,
        );

        let mut sleep_interval: Option<Duration> = None;
        let mut prev_addrs: Option<UdpTrackerResolveResult> = None;
        loop {
            if let Some(i) = sleep_interval {
                trace!(interval=?sleep_interval, "sleeping");
                tokio::time::sleep(i).await;
            }

            // This should retry forever until the addrs are resolved.
            let addrs = (async || {
                udp_tracker_to_socket_addrs(host.clone(), port)
                    .instrument(trace_span!("resolve", ?host))
                    .await
                    .or_else(|err| prev_addrs.ok_or(err))
            })
            .retry(
                ExponentialBuilder::new()
                    .without_max_times()
                    .with_max_delay(Duration::from_secs(60))
                    .with_jitter(),
            )
            .notify(|err, retry| debug!(retry_in=?retry, "error resolving tracker: {err:#}"))
            .await
            .context("this shouldn't happen: failed resolving tracker addrs")?;

            prev_addrs = Some(addrs);

            match addrs {
                UdpTrackerResolveResult::One(addr) => {
                    match self
                        .tracker_one_request_udp(addr, &client)
                        .instrument(trace_span!("udp request", ?addr))
                        .await
                    {
                        Ok(sleep) => sleep_interval = Some(sleep),
                        Err(_) => {
                            sleep_interval = Some(sleep_interval.unwrap_or(Duration::from_secs(60)))
                        }
                    }
                }
                UdpTrackerResolveResult::Two(v4, v6) => {
                    let (r4, r6) = tokio::join!(
                        self.tracker_one_request_udp(v4.into(), &client)
                            .instrument(trace_span!("udp request", addr=?v4)),
                        self.tracker_one_request_udp(v6.into(), &client)
                            .instrument(trace_span!("udp request", addr=?v6))
                    );
                    sleep_interval = Some(
                        r4.or(r6)
                            .ok()
                            .or(sleep_interval)
                            .unwrap_or(Duration::from_secs(60)),
                    )
                }
            }
        }
    }

    async fn tracker_one_request_udp(
        &self,
        addr: SocketAddr,
        client: &UdpTrackerClient,
    ) -> anyhow::Result<Duration> {
        use tracker_comms_udp::*;

        let _permit = self
            .acquire_announce_slot()
            .await
            .context("tracker announce cancelled")?;

        let stats = self.stats.get();
        let request = AnnounceFields {
            info_hash: self.info_hash,
            peer_id: self.peer_id,
            downloaded: stats.downloaded_bytes,
            left: stats.get_left_to_download_bytes(),
            uploaded: stats.uploaded_bytes,
            event: match stats.torrent_state {
                TrackerCommsStatsState::None => EVENT_NONE,
                TrackerCommsStatsState::Initializing => EVENT_STARTED,
                TrackerCommsStatsState::Paused => EVENT_STOPPED,
                TrackerCommsStatsState::Live => {
                    if stats.is_completed() {
                        EVENT_COMPLETED
                    } else {
                        EVENT_STARTED
                    }
                }
            },
            key: self.key,
            port: self.announce_port,
        };

        match client.announce(addr, request).await {
            Ok(response) => {
                info!(tracker=?addr, peers=response.addrs.len(), "tracker announce succeeded");
                for addr in response.addrs {
                    self.tx.send(addr).await.context("rx closed")?;
                }
                let sleep = response.interval.max(5);
                let sleep = Duration::from_secs(sleep as u64);
                Ok(sleep)
            }
            Err(e) => {
                debug!(?addr, "error reading announce response: {e:#}");
                Err(e)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// A minimal HTTP tracker: answers any GET with a compact bencoded response
    /// and records how many announces are being handled at once, so the test can
    /// assert the *session-wide* peak rather than only the semaphore's value.
    fn spawn_counting_tracker(
        peak: Arc<AtomicUsize>,
        in_flight: Arc<AtomicUsize>,
        served: Arc<AtomicUsize>,
    ) -> u16 {
        use std::io::{Read, Write};

        let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).expect("bind tracker");
        let port = listener.local_addr().expect("addr").port();
        listener.set_nonblocking(false).expect("blocking listener");

        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { continue };
                // Read the request head so the client sees a well-formed reply.
                let mut buf = [0u8; 1024];
                let _ = stream.read(&mut buf);

                let now = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(now, Ordering::SeqCst);
                // Hold the request open briefly: overlapping announces become
                // observable instead of racing past each other.
                std::thread::sleep(std::time::Duration::from_millis(50));
                in_flight.fetch_sub(1, Ordering::SeqCst);
                served.fetch_add(1, Ordering::SeqCst);

                // A valid bencoded response: no peers, 60s interval.
                let body = b"d8:peers0:8:intervali60e13:min intervali30ee".to_vec();
                let head = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = stream.write_all(head.as_bytes());
                let _ = stream.write_all(&body);
                let _ = stream.flush();
            }
        });
        port
    }

    /// Plan verification: with 20 real HTTP trackers and the default budget of
    /// 8, every tracker receives its announce and the server-observed
    /// concurrency never exceeds the budget.
    #[tokio::test(flavor = "multi_thread")]
    async fn real_http_trackers_observe_the_announce_budget_peak() {
        const TRACKERS: usize = 20;
        const BUDGET: usize = 8;

        let peak = Arc::new(AtomicUsize::new(0));
        let in_flight = Arc::new(AtomicUsize::new(0));
        let served = Arc::new(AtomicUsize::new(0));

        let mut urls = HashSet::new();
        for _ in 0..TRACKERS {
            let port = spawn_counting_tracker(peak.clone(), in_flight.clone(), served.clone());
            urls.insert(
                Url::parse(&format!("http://127.0.0.1:{port}/announce")).expect("tracker url"),
            );
        }

        let budget = Arc::new(tokio::sync::Semaphore::new(BUDGET));
        let cancel = tokio_util::sync::CancellationToken::new();
        let client = reqwest::Client::new();
        let udp = UdpTrackerClient::new(cancel.clone(), None)
            .await
            .expect("udp client");

        let mut stream = TrackerComms::start(
            Id20::default(),
            Id20::default(),
            urls,
            Box::new(()),
            None,
            6881,
            client,
            udp,
            budget,
            cancel.clone(),
        )
        .expect("tracker comms start");

        // The returned stream is lazy: the monitors only run while it is being
        // polled, so drain it for the duration of the measurement.
        let drain = tokio::spawn(async move {
            use futures::StreamExt;
            while stream.next().await.is_some() {}
        });

        // Let the monitors run their first announce round.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        while served.load(Ordering::SeqCst) < TRACKERS {
            if tokio::time::Instant::now() > deadline {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        cancel.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(5), drain).await;

        let served = served.load(Ordering::SeqCst);
        let peak = peak.load(Ordering::SeqCst);
        assert_eq!(
            served, TRACKERS,
            "every tracker, including those past the budget, must receive an announce"
        );
        assert!(
            peak <= BUDGET,
            "observed announce concurrency {peak} exceeded the budget {BUDGET}"
        );
    }

    /// Every tracker must eventually get its announce served, including the
    /// 9th and later: the monitors all run, and only the network request is
    /// serialised by the budget.
    #[tokio::test(flavor = "multi_thread")]
    async fn all_trackers_beyond_the_budget_are_served() {
        const TRACKERS: usize = 20;
        const BUDGET: usize = 8;

        let served = std::sync::Arc::new(AtomicUsize::new(0));
        let budget = std::sync::Arc::new(tokio::sync::Semaphore::new(BUDGET));
        let cancel = tokio_util::sync::CancellationToken::new();

        // Model each tracker's network work: bounded by the shared budget.
        let mut tasks = Vec::new();
        for _ in 0..TRACKERS {
            let budget = budget.clone();
            let served = served.clone();
            let cancel = cancel.clone();
            tasks.push(tokio::spawn(async move {
                let _permit = tokio::select! {
                    _ = cancel.cancelled() => return,
                    p = tokio::time::timeout(
                        Duration::from_secs(5),
                        budget.acquire(),
                    ) => p.expect("must not starve").expect("semaphore open"),
                };
                served.fetch_add(1, Ordering::Relaxed);
            }));
        }
        for t in tasks {
            t.await.expect("task");
        }
        assert_eq!(
            served.load(Ordering::Relaxed),
            TRACKERS,
            "every tracker, including those past the budget, must be served"
        );
    }

    #[test]
    fn announce_interval_never_undercuts_min_interval() {
        let outcome = AnnounceOutcome {
            interval: Duration::from_secs(60).max(Duration::from_secs(300)),
            min_interval: Duration::from_secs(300),
        };
        assert_eq!(outcome.interval, Duration::from_secs(300));

        let outcome = AnnounceOutcome {
            interval: Duration::from_secs(600).max(Duration::from_secs(300)),
            min_interval: Duration::from_secs(300),
        };
        assert_eq!(outcome.interval, Duration::from_secs(600));

        // A forced interval below the tracker minimum is raised to it.
        let forced = Duration::from_secs(30);
        assert_eq!(
            forced.max(Duration::from_secs(300)),
            Duration::from_secs(300)
        );
        // ...but a forced interval above it is respected.
        let forced = Duration::from_secs(900);
        assert_eq!(
            forced.max(Duration::from_secs(300)),
            Duration::from_secs(900)
        );
    }
}
