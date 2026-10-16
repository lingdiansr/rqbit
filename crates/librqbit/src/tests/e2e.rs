use std::{net::Ipv4Addr, time::Duration};

use anyhow::{Context, bail};
use librqbit_core::magnet::Magnet;
use rand::RngExt;
use tokio::{
    spawn,
    time::{interval, timeout},
};
use tracing::{Instrument, error, error_span, info};

use crate::{
    AddTorrentOptions, AddTorrentResponse, ConnectionOptions, ListenerMode, Session,
    SessionOptions, SessionPersistenceConfig, create_torrent,
    listen::ListenerOptions,
    spawn_utils::BlockingSpawner,
    tests::test_util::{
        DropChecks, TestPeerMetadata, create_default_random_dir_with_torrents,
        create_new_file_with_random_content, setup_test_logging, wait_until_i_am_the_last_task,
    },
};

#[tokio::test(flavor = "multi_thread")]
async fn test_e2e_download_tcp() {
    _test_e2e_download_timeout_and_cleanups(ListenerMode::TcpOnly).await
}

#[tokio::test(flavor = "multi_thread")]
async fn test_e2e_download_utp() {
    _test_e2e_download_timeout_and_cleanups(ListenerMode::UtpOnly).await
}

async fn _test_e2e_download_timeout_and_cleanups(mode: ListenerMode) {
    let timeout = std::env::var("E2E_TIMEOUT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(180);

    let drop_checks = DropChecks::default();
    tokio::time::timeout(
        Duration::from_secs(timeout),
        _test_e2e_download(mode, &drop_checks),
    )
    .await
    .context("test_e2e_download timed out")
    .unwrap();

    // Wait to ensure everything is dropped.
    wait_until_i_am_the_last_task().await.unwrap();

    drop_checks.check().unwrap();
}

async fn _test_e2e_download(mode: ListenerMode, drop_checks: &DropChecks) {
    setup_test_logging();
    match crate::try_increase_nofile_limit() {
        Ok(limit) => info!(limit, "increased ulimit"),
        Err(e) => error!(error=?e, "error increasing ulimit"),
    };

    // 1. Create a torrent
    // Ideally (for a more complicated test) with N files, and at least N pieces that span 2 files.

    let piece_length: u32 = 16384 * 2;
    let file_length: usize = 8 * 1000 * 1000; // uneven files will make pieces cross file boundaries

    // Not setting this too high as 64 causes too many open files on osx on github runners.
    let num_files: usize = 8;

    let tempdir =
        create_default_random_dir_with_torrents(num_files, file_length, Some("rqbit_e2e"));
    let torrent_file = create_torrent(
        dbg!(tempdir.path()),
        crate::CreateTorrentOptions {
            piece_length: Some(piece_length),
            ..Default::default()
        },
        &BlockingSpawner::new(1),
    )
    .await
    .unwrap();

    let num_servers = std::env::var("E2E_NUM_SERVERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(32u8);

    let torrent_file_bytes = torrent_file.as_bytes().unwrap();
    let mut futs = Vec::new();

    // 2. Start N servers that are serving that torrent, and return their IP:port combos.
    //    Disable DHT on each.
    for i in 0..num_servers {
        let torrent_file_bytes = torrent_file_bytes.clone();
        let tempdir = tempdir.path().to_owned();
        let drop_checks = drop_checks.clone();
        let fut = spawn(
            async move {
                let peer_id = TestPeerMetadata {
                    server_id: i,
                    max_random_sleep_ms: rand::rng().random_range(0u8..16),
                }
                .as_peer_id();
                let listen_port = 15100u16 + i as u16;
                let session = crate::Session::new_with_opts(
                    std::env::temp_dir().join("does_not_exist"),
                    SessionOptions {
                        dht: None,
                        peer_id: Some(peer_id),
                        listen: Some(ListenerOptions {
                            mode,
                            listen_addr: (Ipv4Addr::LOCALHOST, listen_port).into(),
                            ..Default::default()
                        }),
                        root_span: Some(error_span!(parent: None, "server", id = i)),
                        disable_local_service_discovery: true,
                        ..Default::default()
                    },
                )
                .await
                .context("error starting session")?;

                drop_checks.add(&session, format!("server session {i}"));

                info!("started session");

                let handle = session
                    .add_torrent(
                        crate::AddTorrent::TorrentFileBytes(torrent_file_bytes),
                        Some(AddTorrentOptions {
                            overwrite: true,
                            output_folder: Some(tempdir.to_str().unwrap().to_owned()),
                            ..Default::default()
                        }),
                    )
                    .await
                    .context("error adding torrent")?;
                let h = handle.into_handle().context("into_handle()")?;

                drop_checks.add(&h.shared, format!("server {i} torrent shared handle"));

                let mut interval = interval(Duration::from_millis(100));

                info!("added torrent");
                loop {
                    interval.tick().await;
                    let is_live = h
                        .with_state(|s| match s {
                            crate::ManagedTorrentState::Initializing(_) => Ok(false),
                            crate::ManagedTorrentState::Live(l) => {
                                if !l.is_finished() {
                                    bail!("torrent went live, but expected it to be finished");
                                }
                                Ok(true)
                            }
                            crate::ManagedTorrentState::Error(e) => bail!("error: {e:#}"),
                            _ => bail!("broken state"),
                        })
                        .context("error checking for torrent liveness")?;
                    if is_live {
                        break;
                    }
                }
                info!("torrent is live");
                let addr = session
                    .listen_addr()
                    .context("expected listen_addr to be set")?;
                Ok::<_, anyhow::Error>((session, addr))
            }
            .instrument(error_span!("server", id = i)),
        );
        futs.push(timeout(Duration::from_secs(30), fut));
    }

    let mut peers = Vec::new();

    // This is around just not to drop.
    let mut _servers = Vec::new();
    for (id, peer) in futures::future::join_all(futs)
        .await
        .into_iter()
        .enumerate()
    {
        let (server, peer) = peer
            .with_context(|| format!("join error, server={id}"))
            .unwrap()
            .with_context(|| format!("timeout, server={id}"))
            .unwrap()
            .with_context(|| format!("server couldn't start, server={id}"))
            .unwrap();
        peers.push(peer);
        _servers.push(server);
    }

    info!("started all servers, starting client");

    let client_iters = std::env::var("E2E_CLIENT_ITERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1usize);

    let magnet = Magnet::from_id20(torrent_file.info_hash(), Vec::new(), None).to_string();

    // 3. Start a client with the initial peers, and download the file.
    for _ in 0..client_iters {
        let root = tempfile::TempDir::with_prefix("rqbit_e2e_client").unwrap();
        let outdir = root.path().join("out");
        let session_persistence = root.path().join("session");
        let session = Session::new_with_opts(
            outdir.to_owned(),
            SessionOptions {
                dht: None,
                persistence: Some(SessionPersistenceConfig::Json {
                    folder: Some(session_persistence),
                }),
                listen: if mode.utp_enabled() {
                    Some(ListenerOptions {
                        mode: ListenerMode::UtpOnly,
                        listen_addr: ([127, 0, 0, 1], 15099).into(),
                        ..Default::default()
                    })
                } else {
                    None
                },
                connect: Some(ConnectionOptions {
                    enable_tcp: mode.tcp_enabled(),
                    ..Default::default()
                }),
                fastresume: true,
                disable_local_service_discovery: true,
                root_span: Some(error_span!("client")),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        drop_checks.add(&session, "client session");

        info!("started client session");

        let (id, handle) = {
            let r = session
                .add_torrent(
                    crate::AddTorrent::Url((&magnet).into()),
                    Some(AddTorrentOptions {
                        initial_peers: Some(peers.clone()),
                        // only_files: Some(vec![0]),
                        overwrite: false,
                        ..Default::default()
                    }),
                )
                .await
                .unwrap();

            match r {
                AddTorrentResponse::AlreadyManaged(_, _) => todo!(),
                AddTorrentResponse::ListOnly(_) => todo!(),
                AddTorrentResponse::Added(id, h) => (id, h),
            }
        };

        info!("added handle");

        {
            let stats_printer = {
                let handle = handle.clone();
                async move {
                    let mut interval = interval(Duration::from_millis(100));

                    loop {
                        interval.tick().await;
                        let stats = handle.stats();
                        let live = match &stats.live {
                            Some(live) => live,
                            None => continue,
                        };
                        let pstats = &live.snapshot.peer_stats;

                        info!(
                            progress_percent =
                                format!("{}", stats.progress_percent_human_readable()),
                            peers = format!("{:?}", pstats),
                        );
                    }
                }
            }
            .instrument(error_span!("stats_printer"));

            let timeout = timeout(Duration::from_secs(180), handle.wait_until_completed());

            tokio::pin!(stats_printer);
            tokio::pin!(timeout);

            let mut stats_finished = false;
            loop {
                tokio::select! {
                    r = &mut timeout => {
                        r.unwrap().unwrap();
                        break;
                    }
                    _ = &mut stats_printer, if !stats_finished => {
                        stats_finished = true;
                    }
                }
            }
        }

        info!("handle is completed");
        tokio::time::timeout(Duration::from_secs(5), session.delete(id.into(), false))
            .await
            .context("timeout deleting torrent")
            .unwrap()
            .context("error deleting")
            .unwrap();

        info!("deleted handle");

        // 4. After downloading, recheck its integrity.
        let handle = session
            .add_torrent(
                crate::AddTorrent::TorrentFileBytes(torrent_file_bytes.clone()),
                Some(AddTorrentOptions {
                    paused: true,
                    overwrite: true,
                    ..Default::default()
                }),
            )
            .await
            .unwrap()
            .into_handle()
            .unwrap();

        info!("re-added handle");

        timeout(Duration::from_secs(30), async {
            let mut interval = interval(Duration::from_millis(100));
            loop {
                interval.tick().await;
                let b = handle
                    .with_state(|s| match s {
                        crate::ManagedTorrentState::Initializing(_) => Ok(false),
                        crate::ManagedTorrentState::Paused(p) => {
                            assert_eq!(p.chunk_tracker.get_hns().needed_bytes, 0);
                            Ok(true)
                        }
                        _ => bail!("bugged state"),
                    })
                    .unwrap();
                if b {
                    break;
                }
            }
        })
        .await
        .unwrap();

        tokio::time::timeout(
            Duration::from_secs(5),
            session.delete(handle.id().into(), true),
        )
        .await
        .context("timeout")
        .unwrap()
        .context("error deleting")
        .unwrap();

        // Ensure the files were deleted from the filesystem.
        let d = std::fs::read_dir(&outdir)
            .context("read_dir outdir")
            .unwrap();
        assert_eq!(
            d.into_iter().count(),
            0,
            "{outdir:?} was not empty after deletion"
        );

        info!("all good");
    }
}

/// A `.bitv` that claims a piece the payload does not actually contain must be
/// rejected by `validate_fastresume`, not silently trusted.
///
/// Regression for the upstream `check_piece(..).is_err()`-only check: a SHA1
/// mismatch returns `Ok(false)` and used to be treated as "piece is fine",
/// restoring a phantom piece that is never re-downloaded and leaving a
/// zero-filled hole in the "finished" file.
/// A `.bitv` that claims a piece the payload does not actually contain must be
/// rejected by `validate_fastresume`, not silently trusted.
///
/// Regression for the upstream `check_piece(..).is_err()`-only check: a SHA1
/// mismatch returns `Ok(false)` and used to be treated as "piece is fine",
/// restoring a phantom piece that is never re-downloaded and leaving a
/// zero-filled hole in the "finished" file.
#[tokio::test]
async fn test_fastresume_rejects_phantom_have_piece() -> anyhow::Result<()> {
    setup_test_logging();

    let piece_length: u32 = 32 * 1024;
    let file_length: usize = 300_000; // 10 pieces, last one partial

    // A single-file torrent is laid out as `<parent>/<file name>`, and
    // `create_torrent`'s output folder is that parent -- so pointing the
    // session's output folder at it puts the payload exactly where the torrent
    // expects it, with no network involved.
    let dir = tempfile::TempDir::with_prefix("rqbit_fastresume")?;
    let name = "payload.bin";
    let payload_file = dir.path().join(name);
    create_new_file_with_random_content(&payload_file, file_length);
    let outdir = dir.path().to_owned();

    let torrent_file = create_torrent(
        &payload_file,
        crate::CreateTorrentOptions {
            piece_length: Some(piece_length),
            ..Default::default()
        },
        &BlockingSpawner::new(1),
    )
    .await?;
    let torrent_bytes = torrent_file.as_bytes()?;

    let persistence = tempfile::TempDir::with_prefix("rqbit_fastresume_session")?.keep();
    std::fs::create_dir_all(&persistence)?;
    let session_opts = || SessionOptions {
        dht: None,
        persistence: Some(SessionPersistenceConfig::Json {
            folder: Some(persistence.clone()),
        }),
        disable_local_service_discovery: true,
        fastresume: true,
        ..Default::default()
    };

    // 1. First run over the good payload: the initial check must find every
    //    piece and persist that fact as a `.bitv`.
    {
        let session = Session::new_with_opts(outdir.clone(), session_opts())
            .await
            .context("start first session")?;
        let handle = match session
            .add_torrent(
                crate::AddTorrent::TorrentFileBytes(torrent_bytes.clone()),
                Some(AddTorrentOptions {
                    paused: true,
                    overwrite: true,
                    ..Default::default()
                }),
            )
            .await
            .context("add to first session")?
        {
            AddTorrentResponse::Added(_, handle) => handle,
            AddTorrentResponse::AlreadyManaged(_, handle) => handle,
            AddTorrentResponse::ListOnly(_) => bail!("unexpected list-only response"),
        };

        let needed = timeout(Duration::from_secs(60), wait_needed_bytes(&handle))
            .await
            .context("timed out waiting for the first initial check")?;
        assert_eq!(needed, 0, "first run must see a complete payload");

        // No `delete`: that would remove the `.bitv` along with the rest of
        // the persistence entry. Dropping the session is enough to release it.
        drop(handle);
        drop(session);
    }

    // The `.bitv` is keyed by info hash and preserved by `delete(.., false)`.
    let bitv_path = std::fs::read_dir(&persistence)?
        .filter_map(Result::ok)
        .map(|e| e.path())
        .find(|p| p.extension().is_some_and(|x| x == "bitv"))
        .context("first run must have written a .bitv")?;
    let actual_bitv_len = std::fs::metadata(&bitv_path)?.len();
    let expected_bitv_len = (file_length as u64)
        .div_ceil(u64::from(piece_length))
        .div_ceil(8);
    assert_eq!(
        actual_bitv_len, expected_bitv_len,
        "unexpected .bitv length at {bitv_path:?}"
    );

    // 2. Forge the exact phantom state: the `.bitv` claims to have every piece
    //    while piece 0 on disk is corrupted. `validate_fastresume` always
    //    checks the first have-piece of each file, so it must hit piece 0 and
    //    fall back to a full check instead of trusting the forge.
    let bitv_len = usize::try_from(actual_bitv_len).context("bitv length fits in usize")?;
    std::fs::write(&bitv_path, vec![0xFFu8; bitv_len])?;
    {
        use std::io::{Seek, SeekFrom, Write};
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .open(&payload_file)
            .context("open payload to corrupt")?;
        f.seek(SeekFrom::Start(0))?;
        f.write_all(&[0xA5u8; 4096])?;
        f.sync_all()?;
    }

    // 3. Second run with the same payload + persistence.
    let session = Session::new_with_opts(outdir.clone(), session_opts())
        .await
        .context("start second session")?;
    let handle = session
        .add_torrent(
            crate::AddTorrent::TorrentFileBytes(torrent_bytes),
            Some(AddTorrentOptions {
                paused: true,
                overwrite: true,
                ..Default::default()
            }),
        )
        .await
        .context("add to second session")?
        .into_handle()
        .context("expected a handle, not a list-only response")?;

    let needed = timeout(Duration::from_secs(60), wait_needed_bytes(&handle))
        .await
        .context("timed out waiting for the second initial check")?;

    assert!(
        needed >= u64::from(piece_length),
        "corrupted piece 0 was restored from the forged .bitv as have; \
         validate_fastresume accepted a hash mismatch (needed_bytes={needed})"
    );

    Ok(())
}

/// Resolves once the paused torrent's chunk tracker is available, yielding the
/// bytes it still needs (0 == it believes the payload is complete).
async fn wait_needed_bytes(handle: &crate::torrent_state::ManagedTorrentHandle) -> u64 {
    use crate::ManagedTorrentState;

    let mut interval = interval(Duration::from_millis(50));
    loop {
        interval.tick().await;
        let needed = handle.with_state(|s| match s {
            ManagedTorrentState::Paused(p) => Some(p.chunk_tracker.get_hns().needed_bytes),
            _ => None,
        });
        if let Some(needed) = needed {
            return needed;
        }
    }
}
