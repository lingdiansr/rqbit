
## 叠加测试基准（2026-08-27，fd feat/bt-perf-test，Ubuntu 26.04）

从 v0.4.8-rc.5 基线（rqbit 3860dbe0）逐项叠加 perf batch，测速率/hash 失败/空转，定位速率瓶颈。

| 引擎 | rev | 速率 | hash失败 | no-pieces空转 | 结论 |
|---|---|---|---|---|---|
| 基线 | 3860dbe0 | 21–34 | 42 | — | 参照 |
| 基线+batch A（#5首波/#6全局上限/#9） | b7fd3e73 | **35/20/35/35** | 6 | 25 | ✅ batch A 非瓶颈，可上30 |
| full perf（A+B+C+D） | c941f2b5 | 8–14 | 293(#8) | **1067** | 空转是速率瓶颈 |

关键结论：
- **#8 写合并 = hash 失败元凶**（基线+A 无 #8，hash 失败 6 vs full perf 293）。`perf-debug-no-writebuffer`(c3287c1e) 禁 #8 后 0 hash 失败。
- **请求器空转（`no pieces to request` 1067）是速率上不去 30M 的直接原因**，基线+A 仅 25 次。空转由 batch B/C/D 引入（batch A 无空转）。
- we are choked=0（非 tit-for-tat 下载限制）、MSE 非坏数据源（#8 写盘误判）。

记录：fd `feat/bt-perf-test` 分支，stash@{0} 存 perf rev 改动；MSE/TcpOnly 配置见 DB bt_mse_mode / bt_downloader.rs:670。
