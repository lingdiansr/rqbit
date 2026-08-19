//! Message Stream Encryption (MSE) handshake.
//!
//! The module is built incrementally across a PR series, each PR staying
//! within ~500 LoC and keeping the crate compiling:
//! - PR 1: crypto primitives (RC4, DH-768)
//! - PR 2: Rc4Reader/Rc4Writer stream wrappers + outgoing handshake
//! - PR 3: incoming handshake + protocol tests
//! - PR 4: MseMode config (disabled by default) + incoming wiring
//! - PR 5: outgoing wiring + plaintext fallback, then enabled by default

pub mod dh768;
pub mod rc4;
pub mod stream;

use std::time::Duration;

use anyhow::{Context, Result, bail};
use rand::{Rng, RngExt};
use sha1w::{ISha1, Sha1};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tracing::{debug, trace, warn};

use dh768::Dh768;
use rc4::Rc4;
use stream::{Rc4Reader, Rc4Writer};

const BT_PROTOCOL_PREFIX: &[u8; 20] = b"\x13BitTorrent protocol";
const BT_HANDSHAKE_LEN: usize = 68;
const MAX_PAD: usize = 512;
const VC_LEN: usize = 8;
const CRYPTO_RC4: u32 = 2;
const PLAINTEXT_SNIFF_TIMEOUT: Duration = Duration::from_secs(2);
pub enum OutgoingOutcome<R, W> {
    /// MSE handshake completed; use the RC4-wrapped streams.
    Encrypted(Rc4Reader<R>, Rc4Writer<W>),
    /// The peer answered with a plaintext BitTorrent handshake within the
    /// sniff window. Abort MSE and redial plaintext on a fresh connection.
    PlaintextPeer,
}
fn sha1(parts: &[&[u8]]) -> [u8; 20] {
    let mut hash = Sha1::new();
    for part in parts {
        hash.update(part);
    }
    hash.finish()
}

fn xor20(a: &[u8; 20], b: &[u8; 20]) -> [u8; 20] {
    let mut result = [0u8; 20];
    for i in 0..20 {
        result[i] = a[i] ^ b[i];
    }
    result
}
fn derive_keys(secret: &[u8], skey: &[u8], outgoing: bool) -> (Rc4, Rc4) {
    let (encrypt_key, decrypt_key) = if outgoing {
        (
            sha1(&[b"keyA", secret, skey]),
            sha1(&[b"keyB", secret, skey]),
        )
    } else {
        (
            sha1(&[b"keyB", secret, skey]),
            sha1(&[b"keyA", secret, skey]),
        )
    };
    let mut encrypt = Rc4::new(&encrypt_key);
    let mut decrypt = Rc4::new(&decrypt_key);
    encrypt.discard(1024);
    decrypt.discard(1024);
    (encrypt, decrypt)
}
async fn read_encrypted<R: AsyncRead + Unpin>(
    read: &mut R,
    decrypt: &mut Rc4,
    bytes: &mut [u8],
) -> Result<()> {
    read.read_exact(bytes).await?;
    decrypt.apply_keystream(bytes);
    Ok(())
}

fn random_pad(max: usize) -> Vec<u8> {
    let length = rand::rng().random_range(0..=max);
    let mut pad = vec![0u8; length];
    rand::rng().fill_bytes(&mut pad);
    pad
}

/// Probe the responder's first 20 bytes within a short window to detect a
/// plaintext peer (which sends its BT handshake immediately on connect).
///
/// Returns `Ok(Some(prefix))` with the 20 bytes read when they start with the
/// BT protocol prefix (peer is plaintext). Returns `Ok(None)` with the bytes
/// read so far when they do not (they are the leading bytes of the MSE
/// responder's DH public key), or when the window elapsed without a full 20
/// bytes (treat as a slow MSE responder). The partial `sniffed` bytes must be
/// preserved as the prefix of the DH public key.
async fn sniff_plaintext<R: AsyncRead + Unpin>(read: &mut R) -> Result<(Option<Vec<u8>>, Vec<u8>)> {
    let mut sniffed = Vec::with_capacity(BT_PROTOCOL_PREFIX.len());
    let probe = async {
        let mut byte = [0u8; 1];
        while sniffed.len() < BT_PROTOCOL_PREFIX.len() {
            read.read_exact(&mut byte).await?;
            sniffed.push(byte[0]);
        }
        Ok::<_, std::io::Error>(())
    };
    match tokio::time::timeout(PLAINTEXT_SNIFF_TIMEOUT, probe).await {
        Ok(Ok(())) => {
            let is_plaintext = sniffed == BT_PROTOCOL_PREFIX;
            Ok((is_plaintext.then(|| sniffed.clone()), sniffed))
        }
        Ok(Err(e)) => Err(e.into()),
        Err(_elapsed) => {
            // Window elapsed mid-probe: whatever we got is the leading bytes of
            // the responder's public key (or a slow plaintext peer). Continue MSE.
            Ok((None, sniffed))
        }
    }
}

/// Initiate MSE on a connected stream. IA must be the complete 68-byte
/// BitTorrent handshake and is consumed as part of the MSE exchange.
///
/// Before committing to MSE, briefly probes the responder for a plaintext BT
/// handshake (see [`sniff_plaintext`]); if detected, returns
/// [`OutgoingOutcome::PlaintextPeer`] so the caller can redial plaintext
pub async fn outgoing<R, W>(
    mut read: R,
    mut write: W,
    info_hash: &[u8; 20],
    initial_payload: &[u8; BT_HANDSHAKE_LEN],
) -> Result<OutgoingOutcome<R, W>>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let dh = Dh768::generate(&mut rand::rng());
    write.write_all(&dh.public_key_bytes()).await?;
    write.write_all(&random_pad(MAX_PAD)).await?;
    trace!("sent MSE Ya + PadA, probing responder");

    // Sniff for a plaintext responder before committing to MSE.
    let (plaintext, sniffed) = sniff_plaintext(&mut read).await?;
    if plaintext.is_some() {
        debug!(
            "peer answered with plaintext BT handshake within sniff window, falling back to plaintext redial"
        );
        return Ok(OutgoingOutcome::PlaintextPeer);
    }

    let mut server_public = [0u8; 96];
    server_public[..sniffed.len()].copy_from_slice(&sniffed);
    read.read_exact(&mut server_public[sniffed.len()..])
        .await
        .context("disconnected waiting for MSE responder public key")?;
    let secret = dh
        .shared_secret(&server_public)
        .ok_or_else(|| anyhow::anyhow!("MSE degenerate remote DH key"))?;
    let (mut encrypt, decrypt_base) = derive_keys(&secret, info_hash, true);

    write.write_all(&sha1(&[b"req1", &secret])).await?;
    let skey_hash = sha1(&[b"req2", info_hash]);
    let req3 = sha1(&[b"req3", &secret]);
    write.write_all(&xor20(&skey_hash, &req3)).await?;

    let pad_c = random_pad(MAX_PAD);
    let mut encrypted =
        Vec::with_capacity(VC_LEN + 4 + 2 + pad_c.len() + 2 + initial_payload.len());
    encrypted.extend_from_slice(&[0u8; VC_LEN]);
    encrypted.extend_from_slice(&CRYPTO_RC4.to_be_bytes());
    encrypted.extend_from_slice(&(pad_c.len() as u16).to_be_bytes());
    encrypted.extend_from_slice(&pad_c);
    encrypted.extend_from_slice(&(BT_HANDSHAKE_LEN as u16).to_be_bytes());
    encrypted.extend_from_slice(initial_payload);
    encrypt.apply_keystream(&mut encrypted);
    write.write_all(&encrypted).await?;

    // PadB is raw bytes. Scan with cloned decrypt states so rejected offsets do
    // not consume the formal RC4 stream; commit only after encrypted VC matches.
    let mut raw = Vec::with_capacity(MAX_PAD + VC_LEN);
    let mut candidate_decrypt = None;
    while raw.len() < MAX_PAD + VC_LEN {
        let mut byte = [0u8; 1];
        read.read_exact(&mut byte).await?;
        raw.push(byte[0]);
        if raw.len() >= VC_LEN {
            let offset = raw.len() - VC_LEN;
            let mut candidate = decrypt_base.clone();
            let mut vc = [0u8; VC_LEN];
            vc.copy_from_slice(&raw[offset..]);
            candidate.apply_keystream(&mut vc);
            if vc == [0u8; VC_LEN] {
                candidate_decrypt = Some(candidate);
                break;
            }
        }
    }
    let mut decrypt = match candidate_decrypt {
        Some(d) => d,
        None => {
            warn!("MSE verification constant not found within PadB, aborting handshake");
            bail!("MSE verification constant not found within PadB");
        }
    };

    let mut select = [0u8; 4];
    read_encrypted(&mut read, &mut decrypt, &mut select).await?;
    if u32::from_be_bytes(select) != CRYPTO_RC4 {
        warn!("MSE responder did not select RC4, aborting handshake");
        bail!("MSE responder did not select RC4");
    }
    let mut pad_length = [0u8; 2];
    read_encrypted(&mut read, &mut decrypt, &mut pad_length).await?;
    let pad_length = u16::from_be_bytes(pad_length) as usize;
    if pad_length > MAX_PAD {
        bail!("MSE PadD exceeds {MAX_PAD} bytes");
    }
    let mut pad_d = vec![0u8; pad_length];
    read_encrypted(&mut read, &mut decrypt, &mut pad_d).await?;

    trace!("MSE outgoing handshake complete, RC4 established");
    Ok(OutgoingOutcome::Encrypted(
        Rc4Reader::new(read, decrypt),
        Rc4Writer::new(write, encrypt),
    ))
}

/// Accept either a complete plaintext BitTorrent handshake or MSE. A
/// nonmatching partial plaintext prefix is retained as the beginning of YA.
