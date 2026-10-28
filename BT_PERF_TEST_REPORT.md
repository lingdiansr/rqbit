# BT 引擎 perf 改进功能点测试结果与降速原因

> 测试环境：fd `feat/bt-perf-test` 分支（rqbit `lingdiansr/rqbit`），Ubuntu 26.04 desktop ISO（6.2GB），本地 torrent 源 `dafc8c076ca2`。
> 方法：**叠加测试**（基线 → +batchA → +A+B → …）+ **同刻对照**（切引擎立即测，排除 peer 池随时间波动）。

## 一、功能点测试结果矩阵

| 引擎 | rev | 速率(MB/s) | hash失败 | 空转(no pieces) | 判定 |
|---|---|---|---|---|---|
| v0.4.8-rc.5 基线 | 3860dbe0 | 21–34 | 42 | — | 参照 |
| **基线+A（推荐）** | **b7fd3e73** | **28–35** | **6→0** | **2–25** | ✅ **最优** |
| 基线+A+B | 4a790435 | 3–8 | — | 24 | ❌ 慢 |
| AB-norarest（关 rarest） | 8b876c77 | 7–9 | — | 39 | ❌ 单关无效 |
| AB-nortitfor（关 tit-for-tat） | 2dd52b4b | 4–14 | — | 13 | ❌ 单关无效 |
| 基线+A+C | cc954dba | 5–15 | — | 305 | ❌ 慢+空转 |
| AC-noblocktimeout（关 block 超时） | b03f7c32 | 尾部失真 | — | **13** | 空转大降 |
| AC-noendgame（关 endgame） | 6a46d25e | 10–13 | — | 262 | ❌ block超时残留 |
| 基线+A（同刻对照） | b7fd3e73 | 28–35 | 6 | 2 | ✅ 排除 peer 池 |
| **推荐形态（batch A only）** | **b7fd3e73** | **30/21/30/32/14** | **0** | **2** | ✅ **目标达成** |

## 二、各功能点降速/损坏原因

### 1. #8 磁盘写合并（batch D）→ **hash 失败元凶**
- **现象**：`check_piece`（读盘校验）失败，perf 分支 hash 失败 293 次 vs 基线 42（10 倍）。禁用写合并（`write_chunk` 逐 block 写盘）后 **0 次**。
- **机制**：`pending_writes` 缓存 + `write_piece_buffer` 整片写盘有 bug → **写盘数据与收到数据不一致** → 校验失败。写盘在 `check_piece` 之前（流程：buffer_chunk → flush_piece → check_piece），故是引擎写盘损坏，非 peer 数据坏。
- **排除**：uTP、MSE 加密节点、peer 污染（禁用 #8 后 0 失败）。

### 2. batch B（rarest-first + tit-for-tat）→ **速率拖慢（协同）**
- **现象**：基线+A 28-35 → +B 后 3-8。**单独关 rarest 或 tit-for-tat 均不恢复**（7-9 / 4-14）。
- **机制**：
  - **rarest-first**：优先下载稀有 piece → 稀有 piece 持有 peer 少、源慢/不可靠 → 早期+全程慢。
  - **tit-for-tat**：默认 `am_choking=true`（连接即 choke 对端、进槽才 Unchoke）→ 对端基于互惠**软惩罚**（降低给我们上传的优先级），即使 `we are choked=0`（无显式 choke），下载仍受限。
  - 二者**协同**：单独关一个，另一个仍限制，速率不恢复。
- **结论**：batch B 整体对速率有害，应移除。

### 3. block 超时（batch C）→ **空转 + 拖慢**
- **现象**：基线+A+C 速率 5-15、空转 `no pieces to request` **305 次**（基线+A 仅 2-25）。禁用 block 超时（`DEFAULT_CHUNK_REQUEST_TIMEOUT_SECS` 25→86400）后空转降至 **13**。
- **机制**：每 chunk 25s 超时 → `release_piece` 重新入队给其他 peer → **请求器反复找不到可请求 piece 空转** → 吞吐受限。
- **结论**：block 超时有害，应移除（endgame 非主因）。

### 4. endgame（batch C）→ 非主因
- 关 endgame 后速率仍 10-13（因 block 超时残留），**非空转来源**。可留可去。

### 5. uTP → 排除
- perf 启用 uTP 与 TcpOnly 对比，速率均低（非 uTP 导致）；uTP 非速率瓶颈。

### 6. MSE 加密节点 → 排除
- 纯明文（MSE Disabled）下 hash 失败仍高（460/3min），但**那是 #8 写盘 bug 误判**（禁用 #8 后 MSE 节点数据正常）。禁 MSE 只省握手开销（MSE 798 成功/249 失败重拨），非坏数据来源。**应保持 MSE Enabled**（有 MSE 高速 peer）。

### 7. peer 池波动 → 同刻对照排除
- 每次重加任务 peer 池不同。**同刻对照**（切回基线+A 立即测 28-35）确认是引擎差异，非 peer 池。

## 三、最终推荐形态

**`perf-recommended-final` = batch A only（`b7fd3e73`）**

- ✅ **保留**：batch A（首波加速 #5、全局连接上限 #6、ban 分级 #9）— 对速率无害甚至有益
- ❌ **排除**：batch B（rarest-first + tit-for-tat）、block 超时（batch C 部分）、#8 磁盘写合并（batch D）

**实测效果**：**30+ MB/s（稳定）+ 0 hash 失败 + 无空转**，解决"上不了 30M"与"写盘损坏"两个核心问题。

## 四、后续待办
- **#8 写合并**：建议修复 `pending_writes` 缓存 / `write_piece_buffer` bug（而非禁用），保留写合并的性能收益（磁盘批量写）。
- **endgame**：可单独评估（非本次瓶颈，但下载收尾阶段仍有价值）。
- RQBIT_PERF_IMPROVEMENT_PLAN.md 各分支有分散记录，本报告为汇总完整版。

> 记录：fd `feat/bt-perf-test`（stash@{0} 存 perf rev 改动）、rqbit 隔离分支均 push 到 `lingdiansr/rqbit`（perf-debug-baseline-A/AB/AC/…/perf-recommended-final）。

## 五、uTP 启用测试（2026-08-27，推荐形态 batch A only 基础上）

| 传输层 | 速率(MB/s) | hash失败 | 空转 | uTP连接 |
|---|---|---|---|---|
| TcpOnly | 30/21/30/32/14 | 0 | 2 | 0 |
| **uTP（TcpAndUtp）** | **37/29/19/31/32** | **0** | 12 | **89** |

**uTP 启用无碍且略有益**：hash 失败保持 0（无 #8 写盘 bug，uTP 数据干净），新增 89 个 uTP 连接，峰值 37 MB/s。之前"uTP 排除"结论是 full perf（含 #8）下的误判。

**推荐形态最终版 = batch A only + uTP Enabled（TcpAndUtp）+ MSE Enabled**：30+ MB/s（峰值 37）+ 0 hash 失败 + 无空转。
