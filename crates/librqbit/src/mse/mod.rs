//! Message Stream Encryption (MSE) handshake.
//!
//! The module is built incrementally across a PR series, each PR staying
//! within ~500 LoC and keeping the crate compiling:
//! - PR 1: crypto primitives (RC4, DH-768)
//! - PR 2: Rc4Reader/Rc4Writer stream wrappers + outgoing handshake
//! - PR 3: incoming handshake + protocol tests
//! - PR 4: MseMode config (disabled by default) + incoming wiring
//! - PR 5: outgoing wiring + plaintext fallback, then enabled by default

pub mod rc4;
