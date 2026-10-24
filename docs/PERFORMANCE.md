# 性能测量

性能结论必须带机器、构建、负载与失败口径。现有局部测量支持继续使用单进程和 SQLite，
不构成生产容量或默认并发 64 的调优证明。当前仍需代表性 Linux/TLS、持久磁盘和持续混合负载。

## 可复现的本地对照

使用已经构建的 release 二进制和假数据；脚本不自动下载、构建或连接真实供应商。

```sh
cargo build --release --locked -p ciallopa --bin ciallopa --example loadbench
python3 scripts/bench-local.py --build-ref "$(git rev-parse HEAD)"
```

默认 32/64 并发，各比较直连 mock、网关 usage 关闭、网关 usage 开启。
每个场景使用新进程，一次请求预热后进行一次短突发；它不是持续负载，也没有预热全部连接池。
网关保留默认请求压缩，直连发送明文，因此对照包含压缩工作，不是相同的上游网络字节。
需要检查顺序效应时使用 `--rounds 2`。

脚本保存配置、版本声明、二进制摘要、结果和自有进程退出状态。`--build-ref` 不能单独证明旧二进制
来自该提交。结果同时检查全部错误、拒绝数和仅成功请求的延迟；退出 0 只表示实验完整运行，
不表示全部负载成功。RSS 分开采样 mock 和 gateway，并记录采样峰值与内核 VmHWM。

`loadbench` 是固定并发的闭环工具：响应变慢会降低到达率。评估过载时还要独立增加工作，
观察及时拒绝、管理面响应和取消释放；不能由闭环吞吐单独推导过载能力。
脚本的 cap=1 生命周期场景验证 Body 期间持有租约，但小正文不足以证明真实 TCP 背压。

## SQLite 查询与记录

只使用新生成的数据库。`usage_bench` 会启动 writer 并可能执行保留期维护，不能指向生产 usage 库。

```sh
usage_perf_dir="$(mktemp -d)"
cargo build --release --locked -p ciallopa-usage --example usage_bench
/usr/bin/time -v target/release/examples/usage_bench gen "$usage_perf_dir/1m.db" 1000000 30 8 4
/usr/bin/time -v target/release/examples/usage_bench query "$usage_perf_dir/1m.db"
```

同时观察 summary、筛选 count、ID 游标页、写入队列和清理；复杂错误详情、价格表、高基数模型、
多个查询者以及冷缓存需要独立负载。相同数据在 tmpfs 和持久磁盘上的结果不能混用。

## 已有测量支持的取舍

以下为 2026-10-23 的局部观察，环境是 Intel Core Ultra 7 270K Plus、WSL2 6.18.33.2、
24 逻辑 CPU、约 15.3GiB 内存，release 构建；SQLite 3.51.3，数据库在 tmpfs 暖缓存中。
查询实现基线 `9be339e`，并发对照 `0c6b112`，均为历史样本，不是当前 checkout 的持续性能门禁。

- 100 万条、30 天、8 账号/4 模型的合成数据，完整 summary 中位约 344ms，首页 51 条约 0.24ms，
  summary 查询进程峰值约 43MiB。精确分位样本随查询行数增加；未证明多个并发全量查询的内存上界。
- 副本上增加 account/model 索引改善单条件 count，但未普遍改善 summary，库体积约增 44%；
  未测写入与清理代价，因此没有据此修改 schema。概况和趋势已共用一次 summary。
- 首次 64 并发突发明显慢于直连；同实例后续突发的首输出 p50 约 108ms，接近直连 107ms。
  本机探针把主要差异缩小到首次连接扩张中的显式 IPv4 bind，不能推广为所有 Linux 或 TLS 环境，
  因此未调整地址族或兼容性参数。usage 开关没有稳定一致的延迟差异。
- 有限大请求与慢消费者实验支持背压和取消释放；并发数、通道槽数与 WS 消息上限不是进程字节上界。
  下行 WS 写阻塞可能暂时妨碍同一循环读取，但 Force 提供退出；上行写使用有限预取观察 Close/FIN。
  Close 排在大量未消费数据之后仍受 TCP 顺序约束，不承诺无限前瞻取消。
- 内存分配器（0.1.2，musl release，8 CPU，`max_buffered_body_mb = 256`，20MiB 请求体经本地 mock 转发）：
  musl 自带分配器 10×20MiB 峰值 314–325MiB、12×63MiB 峰值 451–510MiB，负载后回落到 16–17MiB，
  但每请求 CPU 约 0.86–0.94ms；mimalloc 每请求 0.535ms，但峰值 575/738MiB 且负载后仍留 330–568MiB；
  jemalloc 默认配置峰值 363–393MiB、留存 56–87MiB；开 `background_thread` 反而抬高峰值。
  采用 jemalloc `narenas:1`：峰值 333–361MiB / 507MiB，10 秒后留存 41–49MiB、30 秒后 23–24MiB，
  每请求 0.527ms，小请求 oha c=32 约 8400 rps（musl 约 5700–6100）。空闲 RSS 约 15–16MiB（musl 约 13MiB）。
  MSVC 目标仍用系统分配器。
- 客户端连接开启 `TCP_NODELAY`：之前 Nagle 与客户端延迟 ACK 叠加，c=32 小请求 p50 约 44ms、约 700 rps；
  开启后 p50 约 4–5ms。
- 客户端 WS 消息计入 `max_buffered_body_mb` 后（40 连接 × 12MiB、上游延迟 3 秒、预算 256），
  峰值 RSS 为 1188–1244MiB，基线 1081MiB：计数生效（17–21 个 busy 错误帧，客户端重发后都完成），
  但不降低峰值，因为每个连接的接收、解压和帧缓冲在消息可计数之前就已分配；重发还会延长总时长。
  预算 64、20×12MiB 时峰值 384MiB。WS 的内存上界仍是连接数 × 64MiB 消息上限。

## 下一次测量怎么选

从实际部署的上下文长度、流时长和图像数量取样，交叉 SSE/WS、查询争用、冷/热连接与 usage 开关。
慢读和停读要用实际 socket 证据确认阻塞；同时记录 Send-Q、占用、CPU、RSS、失败/拒绝和最终退出。
合法慢进展的恢复对照与取消实验一起做，避免用短超时把正常背压误当故障。

持续测试需包含稳态、突发、停止接受新工作和恢复。只在看到不可接受的成本后决定字节预算、
受控 blocking、索引或聚合；不先设一组与用户机器无关的 CI 性能阈值。

SQLite 的 [查询计划](https://www.sqlite.org/eqp.html)、[索引选择](https://www.sqlite.org/queryplanner.html)
和 [统计维护](https://www.sqlite.org/lang_analyze.html)用于解释结果；不把计划文本固定为产品协议，
也不在只读请求中强行执行全量 ANALYZE。
