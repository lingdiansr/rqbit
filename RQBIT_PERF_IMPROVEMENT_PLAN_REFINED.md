# Rqbit 引擎侧性能改进细化方案（librqbit）

> 依据 `RQBIT_PERF_IMPROVEMENT_PLAN.md` 方向细化。所有行号已在合并后分支
> `perf-improvements`（`mse-dev` rev 3860dbe0 基线 + 配置暴露）上重新 `grep` 复核，
> 与 plan 原始行号有偏差，以本文件为准。

## 基线（合并后实测）

- 分支：`perf-improvements`（基于 `feat/mse-crypto-primitives` f7c71074，合并 `mse-dev` 3860dbe0，提交 64d12155）。
- `connect_rate` / `handshake_timeout` / `peer_backoff` 配置暴露已在合并线就位：
  `PeerConnectionOptions`（peer_connection.rs:73-102）、`ManagedTorrentOptions.connect_rate/peer_backoff`
  （torrent_state/mod.rs:118-121）、`Session.peer_backoff`（session.rs）。
- MSE 采用 mse-dev 实现（自实现 DH768/RC4 + `connect_with_mse_fallback`，无外部 crypto crate 依赖），
  21 项 MSE 测试通过。

## 参考实现对照（libtorrent 54bc4e9 / transmission c22a63d / aria2 本地源码实测）

方案已对照三引擎源码逐条校准，关键差异见各条；总体结论：**9 条方向均正确**，
但 #1 排序 key 与 #3 粒度需要修正，另补三处增强（#2 随机盐 / #4 动态超时 / #9 短时禁连）。
无任何算法级 crate 可用（见文末「Crate 结论」），全部自研；基础设施依赖 rqbit 已具备。

## 逐条细化

---

### #1 tit-for-tat / choke 管理（unchoke 槽 + 周期 rechoke）

**现状**

- `peer_connection.rs:471-478`：握手后无条件发送 `Message::Unchoke`，此后从不主动 choke。
- `live/mod.rs:1489-1534` `on_download_request`：只校验 chunk 可上传（`is_chunk_ready_to_upload`），不校验我方是否 choke 了对方。
- 上传量统计已存在：`PeerCountersAtomic.uploaded_bytes`（live/peer/stats/atomic.rs:14），全量累计、无回合基线。
- 周期任务模式参考：`task_send_pex_to_peer`（live/mod.rs:923-1005，interval 循环）。

**改动**

1. `PeerConnectionHandler` trait（peer_connection.rs:43-71）加方法 `fn should_unchoke(&self) -> bool { false }`（默认 choked）；
   peer_connection.rs:471 改为按返回值发 `Unchoke` 或 `Choke`（模式同 `should_send_bitfield` 的 457 分支）。
2. `LivePeerState`（live/peer/mod.rs:255-280）加字段：
   - `am_choking: bool`（我方当前是否 choke 该 peer，初始 true）
   - `last_rechoke_fetched: u64`（本回合从对方下载字节基线，互惠排序用）
   - `last_rechoke_uploaded: u64`（本回合上传字节基线）
3. 新任务 `task_rechoke`（spawn 于 live/mod.rs:334 附近的任务列表，10s 周期，间隔可配）：
   - **排序 key（对齐 libtorrent `compare_peers` / transmission `get_rate`）**：
     下载中（leech）= 本回合从对方下载字节数（`fetched_bytes - last_rechoke_fetched`，**互惠优先**，
     tit-for-tat 核心）；作种（seed）= 本回合上传字节数（`uploaded_bytes - last_rechoke_uploaded`）；
     私有种子=两者之和。更新基线；
   - 按 key 降序取 top N（默认 8，可配 `unchoke_slots`），另随机选 1 个槽外的感兴趣 peer 作乐观
     unchoke（transmission 对齐：乐观 peer 免疫 N=4 个 rechoke 周期，libtorrent `optimistic_unchoke_interval=30s`）；
   - 对齐细节：纯作种 peer（对端只上传不下）直接 choke；我方上传带宽 maxed out 时不再新 unchoke；
   - 出槽 peer：若 `am_choking == false` → 发送 `WriterRequest::Message(Message::Choke)`，置 `am_choking = true`；
   - 进槽 peer：若 `am_choking == true` → 发送 `Unchoke`，置 `false`。
4. `on_download_request`（live/mod.rs:1489）开头加 `self.state.peers.with_live(self.addr, |live| live.am_choking)` 检查：
   choked 则忽略请求（trace 日志），不发数据。

**交互/风险**

- 仅影响上传方向；下载侧 `i_am_choked` 由对端决定，不动。
- 只下载不上传的 peer（如种子抢速）会被定期 choke，符合 tit-for-tat 预期；对纯下载场景（宿主主流场景）
  我方仍是全量种子时按上传速率排序，全部进槽，行为退化到现状。
- 乐观槽随机化需在 rechoke 任务内用 `rand`（仓库已有该依赖）。
- **排序修正依据**：libtorrent `compare_peers`（choker.cpp:28-40）首选 key 是 `downloaded_in_last_round()`
  （本回合互惠下载量），仅 seed 模式（`unchoke_compare_rr`/`unchoke_compare_fastest_upload`）才用
  `uploaded_in_last_round()`；transmission `get_rate`（peer-mgr.cc:2210-2220）下载中=PeerToClient 下载速率、
  作种=ClientToPeer 上传速率、私有=两者之和。**「按上传量排序」是错误实现，必须互惠优先。**

**验证**

- 日志出现周期 rechoke 事件；只上传不下载的 peer 被 choke 后其对端停止发请求。

---

### #2 rarest-first piece 选择

**现状**

- `piece_tracker.rs:123-164` `acquire_piece`：1) 10x 慢 peer steal → 2) `priority_pieces`（流式优先）→
  3) `iter_queued_pieces`（chunk_tracker.rs:236-249，按文件优先级迭代）→ 4) 3x steal。
- 调用方 `acquire_next_piece`（live/mod.rs:1427-1462）：持有 `live.bitfield`，闭包 `peer_has_piece: |p| bf.get(p)`。
- 无 piece→持有 peer 数统计。

**改动**

1. `PeerStates`（live/peers/mod.rs:19-28）加：
   - `pub fn piece_rarity_counts(&self, total_pieces: usize) -> Vec<u32>`：遍历 live peers 的 `bitfield`，
     set 位累加（O(pieces×peers) 位操作；transmission `count_piece_replication` peer-mgr.cc:448-456 同款实现）；
   - `rarity_cache: Mutex<Option<(Instant, Vec<u32>)>>`，TTL 1s（acquire 是每 peer 每 chunk 一次，必须缓存）。
2. `AcquireRequest`（piece_tracker.rs:49-69）加字段 `piece_rarity: S2: Fn(ValidPieceIndex) -> u32`；
   `acquire_piece` 中 queued 收集后按 rarity 升序稳定排序（稀有度相同保留文件优先级序）。
3. `acquire_next_piece`（live/mod.rs:1450-1462）传入 `piece_rarity: |p| rarity_counts[p.get()]`（从 `state.peers` 取缓存）。

**交互/风险**

- `priority_pieces`（流式）绝对优先保留，rarity 只作用于普通队列顺序。
- rarity=0（无 peer 持有）的 piece 优先——对刚启动、peer 少时效果显著。
- 位图统计在 `lock_write` 内做会放大锁持有时间；在 `with_live_mut` 闭包内通过 `state.peers` 读取时
  注意锁顺序（现有 `acquire_next_piece` 已是 live_mut → state lock_write 嵌套，保持同序即可）。
- **平局随机化（对齐三引擎）**：稀有度相同时 libtorrent 非 rarest 模式随机起点、transmission 加随机盐
  `salt`（peer-mgr-wishlist.h:51-76）、aria2 用构造时 shuffle 的 `order_` 数组（RarestPieceSelector.cc）。
  稳定排序会造成所有 peer 同时扑向同一批稀有 piece 形成热点——实现时平局键追加
  `rand::random::<u64>()`（或 per-torrent 预生成 shuffle 序），参考 aria2 的 order_ 方案（每 piece 固定
  伪随机优先序，select 时线性扫 order 取最小 count，O(P) 且无排序开销）。

**验证**

- 日志/统计显示低稀有度 piece 先被请求（`piece_selection` 顺序）。

---

### #3 endgame 收尾（busy piece 多 peer 并行）

**现状**

- `PieceTracker.inflight: HashMap<ValidPieceIndex, InflightPiece{peer, started}>`（piece_tracker.rs:169-175）严格一 piece 一 peer；
  只有 `try_steal`（180-218）能换 owner。
- 无 endgame 概念：`acquire_piece` 只从 queue（`iter_queued_pieces` 排除了 inflight）取 piece。

**改动**

1. `InflightPiece` 改为 `{ peers: Vec<(PeerHandle, Instant)> }`（piece_tracker.rs:29-32）：
   - `reserve_piece`（167-177）：piece 不在 inflight → 新建；已在 inflight 且 endgame 中 → 追加 peer（上限 2，可配 `endgame_max_peers_per_piece`）。
   - `try_steal`（195-212）：仍可用，只改 owner 语义为替换首 peer。
2. endgame 判定：`queue_pieces` 中仍 needed 且未 owned 的 piece 数 ≤ 阈值（默认 20 或总数 2% 取小，可配 `endgame_piece_threshold`）时进入 endgame 分支（`acquire_piece` 的 NoneAvailable 之后）：
   - 遍历 inflight pieces，选：`peer_has_piece` 且请求者数 < 2 且不含当前 peer 的，返回 Reserved（追加）。
3. 释放路径适配：
   - `take_inflight`（227-230）：piece 完成时清空全部 peers；
   - `release_pieces_owned_by`（246-261）：移除该 peer 的 entry；peers 变空才 `mark_piece_broken_if_not_have`。
4. chunk 级无需额外去重：每个 peer 的 `inflight_requests` 独立（live/peer/mod.rs:267）；
   piece 完成时 `mark_chunk_downloaded` 的 `PreviouslyCompleted` 分支（live/mod.rs:1907-1911）已幂等。

**交互/风险**

- 依赖 #4 的块级超时释放（peer 死亡已有 `release_pieces_owned_by`；超时释放见 #4）。
- 双 peer 请求同 piece 时，两个 peer 都可能发完整 piece → 带宽浪费有限（endgame 阶段总量小），
  libtorrent/transmission 均接受此行为。
- 与 #2 交互：endgame 分支优先于 rarity 排序（endgame 时 rarity 意义已小）。
- **粒度确认（对齐 libtorrent request_blocks.cpp:178-287）**：libtorrent endgame 是 **block 级**
  （`num_peers(pb)` 同 block 计数、busy block 允许第 2 个 peer 请求）；本方案「endgame 下第二 peer
  acquire 同 piece」在 rqbit 中**天然等价于 block 级双请求**——第二 peer 按 chunk 序请求时与第一 peer
  的请求重叠，先到者写入、后到者 `PreviouslyCompleted`/`LateCanceled` 幂等忽略，无需额外去重。
  aria2 同款：endgame 下全部缺失 block 打乱重请求、跳过 outstanding（DefaultBtRequestFactory.cc）。
  触发条件对齐 libtorrent strict_end_game_mode（有 outstanding 请求时不再 pick busy）。

**验证**

- 下载接近完成时同一 piece 出现 2 个请求 peer。

---

### #4 逐 block 请求超时 + snub

**现状**

- `InflightRequest = ChunkInfo` type alias（live/peer/mod.rs:23）；`inflight_requests: HashSet<InflightRequest>`（267）无时间戳。
- requester（live/mod.rs:1627-1759）发送后不跟踪；对端无响应只能等连接级 `read_write_timeout` 断连。
- `wait_for_request_slot`（1612-1625）依赖 `request_slots_changed` Notify。

**改动**

1. `inflight_requests` 改为 `HashMap<ChunkInfo, Instant>`（记录发送时刻）：
   - `add_inflight_request`（314-316）→ `insert(chunk, Instant::now())`；
   - `remove_inflight_request`（318-332）签名不变，删 key 时 notify；
   - 新增 `pub fn expire_inflight_requests(&mut self, timeout: Duration) -> Vec<ChunkInfo>` 返回超时 chunk。
2. 新任务 `task_chunk_request_timeout_checker`（spawn 于 live/mod.rs:334 附近，1s 周期）：
   - **超时值**：默认固定 25s（可配 `chunk_request_timeout_secs`，对齐 transmission `RequestTimeoutSecs`）；
     **可升级为动态（对齐 libtorrent peer_connection.cpp:4571-4592）**：
     `timeout = avg_piece_download_time + avg/5`，clamp 到 `[2s, 配置上限]`；
     rqbit 已有现成 `PeerCountersAtomic::average_piece_download_time()`（stats/atomic.rs:40-49）可直接复用；
   - 对每个超时 chunk：发送 `Message::Cancel`（可选，节省对端带宽）→ 该 piece 若无其他 peer 在请求
     （endgame 之外）则 `mark_piece_broken_if_not_have` 重入队 + `new_pieces_notify.notify_waiters()`；
   - 该 peer `snubbed_until = now + 60s`（可配 `snub_duration_secs`）：期间 requester 的
     `acquire_next_piece` 返回 None（跳过新 piece），只处理已在途请求。
3. `LivePeerState`（live/peer/mod.rs:255-280）加 `snubbed_until: Option<Instant>` + `chunk_timeouts: u32`。

**交互/风险**

- 与 #3：endgame 下超时 chunk 不重入队（别的 peer 可能已在请求该 piece）。
- `acquire_next_piece`（1427-1431）开头加 snub 检查：`if live.snubbed_until > now { return None }`，
  但保留已有 in-flight 请求的接收（`remove_inflight_request` 路径不受影响）。
- 超时取消后 chunk 重入队需注意：`mark_piece_broken_if_not_have`（chunk_tracker.rs:255-270）会重置
  chunk_status——仅当 piece 已无任何 peer 请求时调用，否则把其他 peer 的进度也清了。
- 连接级 `read_write_timeout` 仍兜底；块级超时是更细粒度、更快恢复的手段。
- 参考实现：libtorrent 动态超时 + snub 标记（desired queue 砍半）；transmission 固定 25s cancel；
  aria2 60s 超时 + `snubbing(true)` 标记（DefaultBtMessageDispatcher.cc）。

**验证**

- 人为挂起某 peer 响应，25s 后该 chunk 重调度到其他 peer（日志可见 re-request）。

---

### #5 首波加速（首批 peer 跳过 connect_rate 节流）

**现状**

- `task_peer_adder`（live/mod.rs:603-673）：每收一个 addr 处理完即 `sleep(connect_interval)`（669-671）。

**改动**

- `ManagedTorrentOptions`（torrent_state/mod.rs:110-130）加 `first_wave_peers: Option<u32>`（默认 30）；
  `task_peer_adder` 加本地计数器 `first_wave_remaining`：>0 时递减并跳过 sleep，=0 后恢复节流。
- `connect_interval == None` 时本就无限速，逻辑不受影响。

**交互/风险**

- 纯本地计数，无并发问题；首波后恢复节流即回落。
- 首个 tracker 响应 / DHT 首批结果通常一次涌入多个 addr，正好对应首波窗口。
- 对齐：libtorrent `torrent_connect_boost=30`（首个 tracker 响应立即并发连 30 个，torrent.cpp:4050-4093）；
  本方案为等价简化（跳过 sleep = 快速发出）。

**验证**

- 首批 tracker 响应后日志显示瞬时并发连接 > connect_rate 节流值，随后回落。

---

### #6 全局连接上限 + 半开管理（session 级信号量）

**现状**

- `Session.peer_limit: Option<usize>`（session.rs:156）为 per-torrent 语义（AddTorrentOptions 传导，1489）。
- per-torrent `peer_semaphore: Arc<Semaphore>`（live/mod.rs:195，构造于 282）；`task_peer_adder` 663
  `acquire_owned()` → `task_manage_outgoing_peer`（528-601）持有 permit，599 drop。

**改动**

1. `Session`（session.rs:111）加 `pub(crate) global_peer_semaphore: Arc<tokio::sync::Semaphore>`；
   `SessionOptions`（424）加 `global_peer_limit: Option<usize>`（默认 200），`Session::new_with_opts`（584）构造。
2. `task_peer_adder`（663）改双 acquire，先全局后单种子（plan 指定顺序）：
   ```rust
   let global_permit = session.global_peer_semaphore.clone().acquire_owned().await?;
   let permit = state.peer_semaphore.clone().acquire_owned().await?;
   ```
3. `task_manage_outgoing_peer` 签名加 `global_permit: OwnedSemaphorePermit`，结束时一并 drop。

**交互/风险**

- 全局先 acquire、per-torrent 后 acquire → 无死锁（per-torrent 永远在全局之后释放）。
- 第一版仅覆盖 outgoing（peer_adder 路径）；inbound 仍由 per-torrent `peer_limit`/seen 检查控制，
  避免削弱可连接性。可选扩展：inbound 用 `try_acquire`，失败即断开。
- 对齐：libtorrent `connections_limit=200`（超限断随机 peer）；transmission `peer_limit_global=200`
  （留 5% 槽位给入站）；aria2 每 torrent 55 + 周期建连 5。本方案信号量实现与 transmission 语义最接近。

**验证**

- 多 torrent 并发时总 outgoing 连接数不超过全局信号量（默认 200）。

---

### #8 磁盘写缓存合并（同 piece 攒批落盘）

**现状**

- `FileOps::write_chunk`（file_ops.rs:310-362）：每个 chunk 直接 `pwrite_all_vectored`（344）；
  `check_piece` 在 live/mod.rs:1931-1934 调用。`FileOps` 由 `state.file_ops()`（688-690）每次新建 → 缓存不能放 FileOps 内。

**改动**

1. `TorrentStateLive` 加 `pending_writes: Mutex<HashMap<ValidPieceIndex, Vec<u8>>>`（piece 数据缓冲，按 piece 索引）；
   `write_chunk` 改为写入缓冲（拷贝），返回后由写合并层调度。
2. flush 策略（libtorrent `store_buffer` 语义，mmap_disk_io.cpp:194-198）：
   - piece 完成（`ChunkMarkingResult::Completed`，live/mod.rs:1902）→ 整体一次 `pwrite`（单次大写，含相邻 block）；
   - 字节阈值（默认 16 MiB，可配 `write_buffer_max_bytes`，对齐 aria2 `--disk-cache=16M`；libtorrent 背压
     `max_queued_disk_bytes=100MB`）或时间阈值（1s）→ flush 最旧 piece；
   - `check_piece`（1931）前 flush 该 piece（hash 校验读盘必须见最新数据）。
3. `mark_piece_hash_failed`（#9）路径清空该 piece 缓冲（避免坏数据滞留）。
4. pause / 删除 / 退出时 flush 全部（`into_chunks` / `drop` 路径）。
5. 统计：`pwrite_count` / `pwrite_bytes` counter，验证批量落盘。

**交互/风险**

- 崩溃丢未 flush 数据：BitTorrent 语义允许（重下该 piece）；fastresume 不受影响（have 位图基于已校验 piece）。
- 内存 = 进行中 piece 数 × piece 大小；用字节阈值兜底（超限强制 flush 最旧，即使 piece 未满）。
- 与 #9：hash 失败后该 piece 缓冲必须清空，否则再次校验读到旧数据。
- 参考实现：libtorrent `store_buffer`（读/哈希路径可从排队写直接取，`set_max_size` 背压）；
  aria2 `WrDiskCacheEntry::append()` 合并相邻连续块直到 capacity、超限 clock 序 flush 最旧（WrDiskCache.cc）；
  transmission 无写缓存（直接 pread/pwrite）。

**验证**

- 统计显示单次 pwrite 大小 > 单 block 16KB。

---

### #9 校验失败 ban 分级（bad_piece_count）

**现状**

- live/mod.rs:1977-1990：hash 失败 → `mark_piece_hash_failed` + `anyhow::bail!("i am probably a bogus peer. dying.")` 直接断连。
- 单次坏数据（网络损坏、偶发坏 peer）即断连，过度敏感。

**改动**

1. `PeerCountersAtomic`（live/peer/stats/atomic.rs:12-26）加 `bad_pieces: AtomicU32`。
2. hash 失败分支（1977-1990）改为：
   ```rust
   counters.bad_pieces.fetch_add(1, Ordering::Relaxed);
   state.lock_write("mark_piece_broken").get_pieces_mut()?.mark_piece_hash_failed(...);
   state.new_pieces_notify.notify_waiters();
   if counters.bad_pieces.load(Ordering::Relaxed) > max_bad_pieces_per_peer { // 默认 5，可配
       anyhow::bail!("peer sent too many bad pieces. disconnecting.")
   }
   // 未超限：仅重入队，继续
   ```
   `max_bad_pieces_per_peer: Option<u32>` 放 `ManagedTorrentOptions`（默认 5，对齐 transmission `MaxBadPiecesPerPeer`）。
3. 注意：bail 会触发 `on_peer_died` → `release_pieces_owned_by`（246）；不 bail 时该 peer 的 inflight 保留，
   坏 piece 已由 `mark_piece_hash_failed` 回 queue。

**交互/风险**

- 与 #4：坏数据 peer 通常也超时 → snub + bad_piece 双计数，累计超限才断连，行为更稳。
- 该 peer 后续继续发好数据时计数不衰减（第一版不做衰减；可选：`reset_peer_backoff` 时清零）。
- **断开后短时禁连（对齐 transmission ban 语义）**：strike 超限断开后应避免立即重连同一 peer。
  libtorrent 用 trust_points（<0 拒绝保存/重连）、transmission ban 后从候选池移除。
  rqbit 复用现有 `peer_backoff`（live/peer/stats/atomic.rs:52-67）：bail 断连路径已触发退避，
  验证退避对 hash-fail 断开同样生效；若不生效则显式 `mark_peer_backoff` 一次。
- 计数归属：`bad_pieces` 放 `PeerCountersAtomic`（per-peer），hash 失败分支在 `write_to_disk` 块内
  可经 `counters` 访问（on_received_piece 已持有），无需改 LivePeerState。

**验证**

- 单个坏 piece 不断连；同 peer 累计 >5 才断开。

---

### 可选 #7 每 peer MSE 能力记忆

- `LivePeerState` 加 `mse_supported: Option<bool>`；peer_connection.rs 握手结果（`connect_with_mse_fallback` 返回值）
  通过 handler 回调（如 `on_mse_handshake_result(bool)`）回写。
- 用途：宿主侧连接决策（跳过必然失败的 MSE 尝试）。不阻塞其余条目，最后做。

## 实施批次与顺序

| 批次 | 条目 | 理由 |
|---|---|---|
| A | #5 首波、#6 全局上限、#9 ban 分级 | 相互独立、低风险、改动局部 |
| B | #2 rarest-first、#1 choke 管理 | 调度策略，独立于请求生命周期 |
| C | #4 块超时 → #3 endgame | 强耦合（endgame 依赖超时释放），C 内先 #4 后 #3 |
| D | #8 写合并 | 风险最高（存储语义），最后 |
| E | 可选 #7 | 不阻塞 |

每批次独立提交、独立验证；批次间不互相依赖。

## 新增配置项汇总（均 Option + 默认值，模式同 `connect_rate`）

| 配置 | 默认 | 条目 | 放置 |
|---|---|---|---|
| `unchoke_slots` | 8 | #1 | ManagedTorrentOptions |
| `rechoke_interval_secs` | 10 | #1 | ManagedTorrentOptions |
| `endgame_piece_threshold` | 20（或总数 2% 取小） | #3 | ManagedTorrentOptions |
| `endgame_max_peers_per_piece` | 2 | #3 | ManagedTorrentOptions |
| `chunk_request_timeout_secs` | 25 | #4 | ManagedTorrentOptions |
| `snub_duration_secs` | 60 | #4 | ManagedTorrentOptions |
| `first_wave_peers` | 30 | #5 | ManagedTorrentOptions |
| `global_peer_limit` | 200 | #6 | SessionOptions |
| `write_buffer_max_bytes` | 16 MiB | #8 | SessionOptions |
| `max_bad_pieces_per_peer` | 5 | #9 | ManagedTorrentOptions |

宿主（FluxDown）无对应配置项，默认值生效即可，不破坏现有宿主调用。

## Crate 结论（逐拆分点核查，crates.io API + 本地源码 + 仓库依赖）

**无任何算法级 crate 可用**：rarest-first / tit-for-tat choker / endgame / 逐 block 超时 /
写合并均为各引擎内聚逻辑，crates.io 无现成实现（`piece-picker` 检索仅得位图类型 crate
`enxame-bitfield`；`bittorrent-rs`/`rusty_torrent` 是完整客户端库，无法嵌入 rqbit 引擎）。
全部 9 条自研。基础设施依赖 rqbit **已全部具备，零新增依赖**：

| 拆分点 | 基础设施 | 状态 |
|---|---|---|
| #1 排序 top-N | std `sort_by` / `select_nth_unstable`；`rand 0.10`（乐观槽） | 已有 |
| #2 稀有度统计 | 自研 `bitv`（仓库已有，chunk_tracker 在用） | 已有 |
| #3 endgame | 无（纯逻辑） | — |
| #4 超时定时器 | `tokio::time`（已有，requester/timer 模式复用） | 已有 |
| #5 首波 | 无（本地计数） | — |
| #6 全局信号量 | `tokio::sync::Semaphore`（已有，per-torrent 在用） | 已有 |
| #8 写缓冲 | 无（自研 `Vec<u8>` piece 缓冲）；`io-uring` 收益不确定且引入复杂度，不做 | — |
| #9 计数 | std `AtomicU32`（已有） | 已有 |
| 退避/限速 | `backon 1.5` / `governor 0.10`（已有） | 已有 |

设计参考（非依赖）：`cenkalti/rain`（Go）参数模型 `RequestTimeout` /
`EndgameMaxDownloadersPerPiece` 与方案取值一致，可查其 README 交叉核对。

## 验证清单（逐条）

见各条「验证」；统一起见，验证用日志 + 新增统计 counter（`session_stats` / peer stats 均可观测），
宿主侧回归：`make testserver` 观察下载吞吐与连接行为不劣化。
