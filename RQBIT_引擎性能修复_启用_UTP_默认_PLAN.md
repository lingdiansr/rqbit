# rqbit 引擎性能修复：启用 uTP 默认，放弃有害/非必需调度功能

## Context

rqbit 引擎性能优化分四批叠加测试：batch A（#5 首波加速、#6 全局连接上限、#9 bad-piece 容忍）实测 28–35 MB/s、0 hash 失败、无空转，为最优形态；batch B（rarest-first + tit-for-tat）、batch C（block 超时 + endgame）、batch D（磁盘写合并）叠加后速率降至 3–8 MB/s、空转 305 次、hash 失败 293 次。

当前 HEAD `perf-recommended-final`（b7fd3e73）已只含 batch A，MSE 已默认 `Enabled`（`mse/mod.rs` 无 feature 门控，`session.rs:547` `MseMode::default()`）。剩余唯一代码改动是启用 uTP 默认（`TcpAndUtp`），并对 batch B/C/D 明确放弃（不合并、不修复）。

跨引擎源码核对（libtorrent `54bc4e9` / libtransmission `c22a63d` / aria2）结论：这五个功能点均非「高性能下载（leecher）引擎」的必选项，放弃：

- **rarest-first**：非必选。是 swarm 健康启发式而非下载速度关键。libtorrent 是 piece_picker 的一种排序选项（`rarest_first`）；transmission 是 wishlist 里「稀有度 replication + 随机盐 salt」一个因子（`peer-mgr-wishlist.cc:56,96`），非纯 rarest-first。rqbit 纯 rarest-first 优先抢稀有 piece（持有 peer 少/慢），实测拖慢。
- **tit-for-tat / choker**：对 leecher 非必选。是上传带宽公平机制（seeding 场景）。libtorrent `choker.cpp`（8 槽+乐观 unchoke）、transmission `rechokeUploads`、aria2 `BtChokeMessage` 均有，但 rqbit naive `am_choking=true` 默认 choke 对端 → 对端互惠软惩罚我们的下载。
- **block 超时**：概念有值但非当前必选，且 rqbit 实现错误。libtorrent 是自适应 `request_timeout`（avg+avg/5）+ snub（`peer_connection.cpp:2951,2969`）；transmission 是 `RequestTimeoutSecs=25s` 的 cancel+drop（`peer-msgs.cc:407,482,782`）。rqbit batch C 的固定 25s `release_piece` → 请求器空转（305 次）。rqbit 已有连接级 `read_write_timeout=10s` 兜底。→ 暂缓（需自适应+snub 设计，非 naive 版）。
- **endgame**：非必选。transmission 无 endgame（grep 空）；libtorrent 有（`request_blocks.cpp:181-261`）。仅加速收尾 1–2%。
- **写合并**：非必选。transmission 直接 pread/pwrite 无写缓存（`inout.cc:53` `write_entire_buf`）；libtorrent `store_buffer`、aria2 `WrDiskCache` 有，但仅 HDD/IOPS 收益，SSD 无显著收益。batch D 有数据损坏 bug（见步骤 2）。

真正影响下载速度的是连接性（batch A）+ 传输/协议（uTP、MSE），已实测有效，保留。

## Approach

### 步骤 1：启用 uTP 默认（TcpAndUtp）

**1a. `crates/librqbit/src/listen.rs` `impl Default for ListenerOptions`（约 65–66 行）**
- 删除第 65 行注释 `// TODO: once uTP is stable upgrade default to both`。
- `mode: ListenerMode::TcpOnly` → `mode: ListenerMode::TcpAndUtp`。

**1b. `crates/rqbit/src/main.rs`（CLI 标志，171–176 行）**
将 `--experimental-enable-utp-listen` 改为 `--disable-utp-listen`，与 `--disable-tcp-listen`（160–163 行）对称：
- `#[arg(long = "disable-utp-listen", env = "RQBIT_UTP_LISTEN_DISABLE")]`
- 字段 `enable_utp_listen: bool` → `disable_utp_listen: bool`（默认 `false`）
- 更新 doc 注释为：`Disable listening for incoming connections over uTP. Note that listening over uTP is enabled by default (--disable-utp-listen to disable).`

**1c. `crates/rqbit/src/main.rs`（611–616 行映射）**
```rust
let listen_mode = match (!opts.disable_tcp_listen, !opts.disable_utp_listen) {
    (true, true) => Some(ListenerMode::TcpAndUtp),
    (true, false) => Some(ListenerMode::TcpOnly),
    (false, true) => Some(ListenerMode::UtpOnly),
    (false, false) => None,
};
```
- 默认 `disable_tcp_listen=false, disable_utp_listen=false` → `(true,true)` → `TcpAndUtp`。
- `enable_utp_listen` 仅定义（176 行）与映射（611 行）两处引用，无其他 callsite；干净 cutover，不留别名。

### 步骤 2：放弃 batch B/C/D（决策，无代码改动）

- 不合并任何含 batch B/C/D 的分支：`perf-debug-baseline-AB`、`perf-debug-baseline-AC`、`perf-debug-no-*`、`perf-improvements` 及远端同名分支。
- 保留分支不删除（「暂时放弃」，供未来参考——尤其 block 超时的自适应+snub 设计、写合并的正确实现）。
- 不实现 batch D 写合并修复。其数据损坏机制（读 `4c9ca32c` 确认）：`flush_over_budget` 会 `remove` 未完成 piece 的缓冲并写盘，后续 `buffer_chunk` 对该 piece `or_insert_with(|| vec![0u8; piece_len])` 重建零缓冲，`flush_piece` 最终用零覆盖已逐出的 chunk → hash 失败；另有 `write_piece_buffer` 在 piece 起始恰逢文件边界时 `to_write==0` 提前 `break` 丢数据。写合并非必选，放弃而非修复。

## Critical files & anchors

- `crates/librqbit/src/listen.rs:53,66` — `ListenerOptions.mode` 字段与 `Default` 实现；改默认 `TcpAndUtp`。
- `crates/rqbit/src/main.rs:162-176,611-616` — uTP CLI 标志与 `listen_mode` 映射。
- `crates/librqbit/src/torrent_state/live/mod.rs:623,2017` — batch A 的 `first_wave_peers` / `max_bad_pieces_per_peer` 使用点（确认保留，不动）。

## Verification

1. 编译：`cargo check -p librqbit -p rqbit`，确认无残留 `enable_utp_listen` 引用。
2. e2e：`cargo test -p librqbit e2e`（覆盖 `test_e2e_download_tcp` / `test_e2e_download_utp` / `test_utp_with_another_client` / `test_tcp_with_another_client`；uTP 默认开启后 UtpOnly 与 TcpAndUtp 路径均需通过）。
3. 性能冒烟（复现报告表一/五方法）：`make testserver`（端口 3030），经 HTTP API 添加 Ubuntu 26.04 desktop ISO（6.2GB，本地 torrent 源 `dafc8c076ca2`），观测：下载速率 ≥ 28 MB/s（batch A 基线区间 28–35）、hash 失败 0、`no pieces to request` 空转 < 25、uTP 连接数 > 0。预期峰值 37 MB/s（报告表五）。

## Assumptions & contingencies

- **uTP 默认稳定**：报告表五已实测 `TcpAndUtp`（37/29/19/31/32 MB/s、0 hash 失败、89 uTP 连接）确认无碍且有益，故默认 `TcpOnly` → `TcpAndUtp`。若冒烟测试发现 uTP 引入不稳定（断连/重传激增），回退 `listen.rs` 默认为 `TcpOnly`，仅保留 `--disable-utp-listen` 标志。
- **CLI 标志改名**：`--experimental-enable-utp-listen` → `--disable-utp-listen` 是破坏性 CLI 变更。默认不保留旧别名（该标志为 experimental，无用户报告依赖）；若需兼容旧脚本，新增 `--disable-utp-listen` 并保留旧标志为 deprecated 别名，但默认逻辑仍走 `TcpAndUtp`。
