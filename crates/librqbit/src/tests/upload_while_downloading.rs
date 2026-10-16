//! A downloading peer must serve verified pieces to other peers while it is
//! still downloading, and must keep accounting those uploads.
//!
//! Three sessions, all loopback and DHT/LSD-free:
//!   A — has the complete file and seeds it,
//!   B — downloads from A with a throttled download rate so it stays incomplete,
//!   C — connects only to B and requests a piece B already verified.
//!
//! The interesting assertions are the negative ones: a peer that never asked
//! for anything must not move the upload counters, and bytes must be counted
//! only once they were actually written.

use std::{net::Ipv4Addr, num::NonZeroU32, sync::Arc, time::Duration};

use librqbit_core::magnet::Magnet;
use tracing::{Instrument, error, error_span, info};

use crate::{
    AddTorrentOptions, AddTorrentResponse, ConnectionOptions, ListenerMode, Session,
    SessionOptions, create_torrent,
    limits::LimitsConfig,
    listen::ListenerOptions,
    spawn_utils::BlockingSpawner,
    tests::test_util::{
        create_default_random_dir_with_torrents, setup_test_logging, wait_until,
        wait_until_i_am_the_last_task,
    },
};

/// Download rate applied to B so it cannot finish before C asks for a piece.
const B_DOWNLOAD_BPS: u32 = 256 * 1024;

fn session_opts(mode: ListenerMode, port: u16, span: tracing::Span) -> SessionOptions {
    SessionOptions {
        dht: None,
        listen: Some(ListenerOptions {
            mode,
            listen_addr: (Ipv4Addr::LOCALHOST, port).into(),
            ..Default::default()
        }),
        connect: Some(ConnectionOptions {
            enable_tcp: mode.tcp_enabled(),
            ..Default::default()
        }),
        disable_local_service_discovery: true,
        root_span: Some(span),
        ..Default::default()
    }
}

async fn add_and_complete(
    session: &Arc<Session>,
    torrent_bytes: bytes::Bytes,
    output_folder: &str,
    options: AddTorrentOptions,
) -> anyhow::Result<Arc<crate::torrent_state::ManagedTorrent>> {
    let handle = match session
        .add_torrent(
            crate::AddTorrent::TorrentFileBytes(torrent_bytes),
            Some(options),
        )
        .await?
    {
        AddTorrentResponse::Added(_, h) => h,
        AddTorrentResponse::AlreadyManaged(_, h) => h,
        AddTorrentResponse::ListOnly(_) => anyhow::bail!("unexpected list-only response"),
    };
    let _ = output_folder;
    Ok(handle)
}

async fn test_upload_while_downloading(mode: ListenerMode) {
    setup_test_logging();

    let piece_length: u32 = 32768;
    let file_length: usize = 2 * 1024 * 1024;

    let tempdir = create_default_random_dir_with_torrents(1, file_length, Some("rqbit_upload_e2e"));
    let torrent_file = create_torrent(
        tempdir.path(),
        crate::CreateTorrentOptions {
            piece_length: Some(piece_length),
            ..Default::default()
        },
        &BlockingSpawner::new(1),
    )
    .await
    .expect("create torrent");
    let torrent_bytes = torrent_file.as_bytes().expect("torrent bytes");
    let info_hash = torrent_file.info_hash();

    // A: the complete source.
    let a_session = Arc::new(
        Session::new_with_opts(
            std::env::temp_dir().join("rqbit_upload_e2e_a_data"),
            session_opts(mode, 15300, error_span!(parent: None, "seeder")),
        )
        .await
        .expect("session A"),
    );
    let a_handle = add_and_complete(
        &a_session,
        torrent_bytes.clone(),
        tempdir.path().to_str().unwrap(),
        AddTorrentOptions {
            overwrite: true,
            output_folder: Some(tempdir.path().to_str().unwrap().to_owned()),
            ..Default::default()
        },
    )
    .await
    .expect("add to A");
    wait_until(
        || {
            if a_handle.stats().finished {
                Ok(())
            } else {
                anyhow::bail!("A not finished")
            }
        },
        Duration::from_secs(30),
    )
    .await
    .expect("A must finish verifying");
    let a_addr = a_session.listen_addr().expect("A listen addr");

    // B: downloads from A under a rate limit, so it stays incomplete.
    let b_root = tempfile::TempDir::with_prefix("rqbit_upload_e2e_b").expect("B tempdir");
    let b_session = Arc::new(
        Session::new_with_opts(
            b_root.path().join("data"),
            SessionOptions {
                ratelimits: LimitsConfig {
                    download_bps: NonZeroU32::new(B_DOWNLOAD_BPS),
                    upload_bps: None,
                },
                ..session_opts(mode, 15301, error_span!(parent: None, "downloader"))
            },
        )
        .await
        .expect("session B"),
    );
    let b_handle = match b_session
        .add_torrent(
            crate::AddTorrent::Url(
                Magnet::from_id20(info_hash, vec![a_addr.to_string()], None)
                    .to_string()
                    .into(),
            ),
            Some(AddTorrentOptions {
                initial_peers: Some(vec![a_addr]),
                ..Default::default()
            }),
        )
        .await
        .expect("add to B")
    {
        AddTorrentResponse::Added(_, h) => h,
        other => panic!(
            "unexpected B add response: {:?}",
            std::any::type_name_of_val(&other)
        ),
    };

    // Wait until B has verified at least one piece but is still downloading.
    wait_until(
        || {
            let stats = b_handle.stats();
            let Some(live) = &stats.live else {
                anyhow::bail!("B has no live state yet");
            };
            if live.snapshot.downloaded_and_checked_pieces > 0 && !stats.finished {
                Ok(())
            } else {
                anyhow::bail!("B has no verified piece yet, or already finished")
            }
        },
        Duration::from_secs(120),
    )
    .await
    .expect("B must have a verified piece while still downloading");

    let b_uploads_before = b_handle
        .stats()
        .live
        .as_ref()
        .map(|l| l.snapshot.uploaded_bytes)
        .unwrap_or(0);
    assert_eq!(
        b_uploads_before, 0,
        "nothing may be counted as uploaded before any peer asked for data"
    );

    // C: connects only to B and takes the pieces B already has.
    let c_root = tempfile::TempDir::with_prefix("rqbit_upload_e2e_c").expect("C tempdir");
    let c_session = Arc::new(
        Session::new_with_opts(
            c_root.path().join("data"),
            session_opts(mode, 15302, error_span!(parent: None, "leecher")),
        )
        .await
        .expect("session C"),
    );
    let b_addr = b_session.listen_addr().expect("B listen addr");
    let c_handle = match c_session
        .add_torrent(
            crate::AddTorrent::Url(
                Magnet::from_id20(info_hash, vec![b_addr.to_string()], None)
                    .to_string()
                    .into(),
            ),
            Some(AddTorrentOptions {
                initial_peers: Some(vec![b_addr]),
                ..Default::default()
            }),
        )
        .await
        .expect("add to C")
    {
        AddTorrentResponse::Added(_, h) => h,
        other => panic!(
            "unexpected C add response: {:?}",
            std::any::type_name_of_val(&other)
        ),
    };

    // C must receive real payload from B, and B must still be downloading.
    wait_until(
        || {
            let stats = c_handle.stats();
            if stats
                .live
                .as_ref()
                .is_some_and(|l| l.snapshot.downloaded_and_checked_pieces > 0)
            {
                Ok(())
            } else {
                anyhow::bail!("C has no verified piece from B yet")
            }
        },
        Duration::from_secs(180),
    )
    .await
    .expect("C must obtain verified pieces from B");

    let b_stats = b_handle.stats();
    let b_uploads_after = b_stats
        .live
        .as_ref()
        .map(|l| l.snapshot.uploaded_bytes)
        .unwrap_or(0);
    assert!(
        b_uploads_after > 0,
        "B served verified pieces to C, so its upload counter must have moved"
    );
    assert!(
        !b_stats.finished,
        "the rate limit must keep B incomplete for this scenario to be meaningful"
    );
    info!(b_uploads_after, "B uploaded while still downloading");

    // Counting happens when a request is accepted, so it survives C
    // disconnecting before we look. (The transient interest/activity
    // aggregates are covered by a deterministic unit test instead: their value
    // at any instant depends on whether C is still connected.)
    let b_live = b_stats.live.as_ref().expect("B live");
    assert!(
        b_live.snapshot.upload_requests > 0,
        "B accepted upload requests from C, so the counter must be non-zero"
    );

    drop(c_handle);
    drop(c_session);
    drop(b_handle);
    drop(b_session);
    drop(a_handle);
    drop(a_session);
    wait_until_i_am_the_last_task()
        .await
        .expect("all tasks must drain");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_upload_while_downloading_tcp() {
    let timeout = std::env::var("E2E_TIMEOUT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(240u64);
    tokio::time::timeout(
        Duration::from_secs(timeout),
        test_upload_while_downloading(ListenerMode::TcpOnly).instrument(error_span!(
            parent: None,
            "upload_while_downloading_tcp"
        )),
    )
    .await
    .map_err(|_| error!("upload-while-downloading TCP test timed out"))
    .expect("upload-while-downloading TCP test timed out");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_upload_while_downloading_utp() {
    let timeout = std::env::var("E2E_TIMEOUT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(240u64);
    tokio::time::timeout(
        Duration::from_secs(timeout),
        test_upload_while_downloading(ListenerMode::UtpOnly).instrument(error_span!(
            parent: None,
            "upload_while_downloading_utp"
        )),
    )
    .await
    .map_err(|_| error!("upload-while-downloading uTP test timed out"))
    .expect("upload-while-downloading uTP test timed out");
}
