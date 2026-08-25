# Rqbit 引擎侧性能改进计划（librqbit）

> 源自 `FLUXDOWN_VS_RQBIT_性能差异归属分析_PLAN.md` 分离，仅含 **rqbit 引擎侧（`crates/librqbit`）** 的改动计划；宿主（FluxDown）侧改动见原文档。
> 每条差异证据来自 `~/code/bt-engine-peer-perf-comparison.md` 的 10 条对比结论，行号在实现前需在 rqbit 源码重新 `grep` 复核。

## Context

librqbit 相对 libtorrent/libtransmission 的引擎内部调度/协议逻辑缺失项，fd（宿主）无法从库外注入，必须在 rqbit fork 内实现。均为**引擎逻辑**改动，fd 侧无对应配置项（已确认 fd 无 `peer_limit`、无全局信号量、无逐 block 超时字段）。

## Approach（C 组 7 条 + 可选 #7）

每条落点均在 `crates/librqbit/src/`。

- **#1 tit-for-tat / choke 管理**：`peer_connection.rs:440-447`（当前连接即发 `Message::Unchoke` 且永不主动 choke）+ `live/mod.rs:1750-1763`（仅响应对端 choke）。新增：unchoke 槽管理（默认 8 槽）+ 周期 rechoke 任务（10s，复用现有 timer 模式）+ 按本回合上传量排序选 top N + 1 个乐观 unchoke 随机槽。默认 choked、进槽才发 Unchoke。
- **#2 rarest-first piece 选择**：`piece_tracker.rs:114-151` `acquire_piece` + `chunk_tracker.rs:236-249` `iter_queued_pieces`。队列序从「文件优先级迭代」改为「按持有该 piece 的 peer 数（稀有度）升序」；需从 `PeerStates` 的 bitfield 统计 piece→peer_count，无现成等价实现。
- **#3 endgame 收尾**：`piece_tracker.rs:104-110,242-261`（block 严格独占）。接近完成（剩余可请求 piece 数低于阈值）时，允许对 in-flight 的 busy piece 重复请求，每 block 最多 2 peer，peer 死亡/超时才释放。
- **#4 逐 block 请求超时 + snub**：`live/mod.rs:1616-1746` `task_peer_chunk_requester`（`add_inflight_request`/`remove_inflight_request`）。给每个 in-flight chunk 加超时定时器（默认 25s，对齐 transmission `RequestTimeoutSecs`），超时 cancel 该 chunk、重新入队给其他 peer、标记该 peer snubbed。此为引擎逻辑，宿主侧已调的 `handshake_timeout`/`peer_backoff` 是连接级，不等同。
- **#5 首波加速**：`live/mod.rs:602-662` `task_peer_adder`。首批 peer（首个 tracker 响应 / DHT 首批结果）前 N=30 个连接跳过 `connect_interval` sleep（绕过 `connect_rate` 节流），后续恢复节流。节流本身已在宿主侧落地（`connect_rate`），本条只补「首波」。
- **#6 全局连接上限 + 半开管理**：`session.rs:156,473`（现 `peer_limit` 为 per-torrent）。新增 session 级全局 `Arc<Semaphore>`（默认 200），在 `live/mod.rs:281-283` 的 per-torrent `peer_semaphore` 之外再套一层，acquire 先全局后单种子。
- **#8 磁盘写缓存合并**：`file_ops.rs:310-355` `write_chunk`（现直接 `pwrite_all_vectored`）。新增写合并层：同 piece 相邻 block 攒批后一次落盘，读路径命中未落盘写（参照 libtorrent `store_buffer`，`mmap_disk_io.cpp:194-198`）。宿主 `bt_partfile` 自定义 storage 在存储接口层，写调度在引擎，故归本侧。
- **#9 校验失败 ban 分级**：`live/mod.rs:1970-1978`（现 hash 失败即 `mark_piece_hash_failed` + 断开 "bogus peer"）。改为 peer 状态累计 `bad_piece_count`，超过 5（对齐 transmission `MaxBadPiecesPerPeer=5`）才断开，未超限只重入队该 piece。
- **可选 #7 每 peer MSE 能力记忆**（pe_support 类标志）：落点 peer 状态结构 + `peer_connection.rs` 握手结果回写。不阻塞 #7 主项（宿主侧已默认 MSE Enabled）。

## Critical files & anchors

1. `crates/librqbit/src/piece_tracker.rs:114-202` — #2/#3 落点（acquire_piece 策略 + 独占逻辑）。
2. `crates/librqbit/src/torrent_state/live/mod.rs:602-662,1616-1746,1970-1978` — #1/#4/#5/#9 落点（peer_adder / chunk_requester / hash 失败）。
3. `crates/librqbit/src/session.rs:156,473` — #6 落点（peer_limit 字段）。
4. `crates/librqbit/src/file_ops.rs:310-355` — #8 落点（write_chunk）。

## 引擎基线说明

- **`mse-dev`（fd 当前 `rev=3860dbe0` 锁定）**：已暴露 `connect_rate` / `handshake_timeout` / `peer_backoff` 配置项（宿主侧 A 组三项依赖它们），本计划的 #4/#5 相关字段在此线已就位。
- **`feat/mse-crypto-primitives`（`f7c71074`）**：平行线，**不含**上述三项配置暴露。若引擎切到该分支，需先把 `connect_rate` / `handshake_timeout` / `peer_backoff` 暴露移植回来，否则宿主侧已落地调优（`bt_downloader.rs` 的对应字段）编译失败。

## 参数对齐值

#1 的 8 槽、#4 的 25s、#9 的 5 次取 libtorrent/transmission 对齐值（见 `~/code/bt-engine-peer-perf-comparison.md` 矩阵）；实现时可微调但需保持默认值可配。

## Verification（逐条实现后）

- #1：日志出现周期 rechoke 事件；对只上传不下载的 peer 会被 choke。
- #2：低稀有度（持有 peer 少）piece 先被请求（日志/统计 `piece_selection` 顺序）。
- #3：下载接近完成时同一 block 出现 2 个请求 peer。
- #4：人为挂起某 peer 的响应，25s 后该 chunk 被重调度到其他 peer。
- #5：首批 tracker 响应后日志显示瞬时并发连接 > connect_rate 节流值，随后回落。
- #6：多 torrent 并发总连接数不超过全局信号量（默认 200）。
- #8：写日志/统计显示 block 批量落盘（单次 pwrite 大小 > 单 block 16KB）。
- #9：单个坏 piece 不立即断 peer；同 peer 累计 >5 才断开。
