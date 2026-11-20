# P1 Linux 对照实验

从仓库根目录运行。需要 Linux x86-64、持久 ext4、GNU binutils、Rust、uv、OpenSSL、支持 H2 的 curl、ss、pidstat 和 iostat。脚本使用 127.0.0.1:18380 和 9443、一次性 CA 与假账号。每个 case 使用独立库。A 的探针依赖带符号的 Linux PIE 构建及父进程跟踪自己子进程的能力。B 直接启动网关。

```bash
export LAB=$(mktemp -d /var/tmp/ciallopa.p1.XXXXXX)
findmnt -T "$LAB"
bash scripts/bench/prepare.sh
export CARGO_TARGET_DIR="$LAB/target"
mkdir "$LAB/old"
git archive v0.6.1 | tar -x -C "$LAB/old"
cargo build --release --locked -p ciallopa --bin ciallopa
cp "$CARGO_TARGET_DIR/release/ciallopa" "$LAB/next-bin"
(cd "$LAB/old" && cargo build --release --locked -p ciallopa --bin ciallopa &&
 cargo build --release --locked -p ciallopa-usage --example usage_bench)
cp "$CARGO_TARGET_DIR/release/ciallopa" "$LAB/old-bin"
cp "$CARGO_TARGET_DIR/release/examples/usage_bench" "$LAB/old-gen"
"$LAB/old-gen" gen "$LAB/seed.db" 1000000 7 100 500 --threads 100000 --quota-every 100 > "$LAB/gen.json"
sha256sum "$LAB/old-bin" "$LAB/next-bin" "$LAB/old-gen" "$LAB/seed.db" > "$LAB/binaries.sha256"
bash scripts/bench/run.sh A B > "$LAB/run.log" 2>&1
bash scripts/bench/cancel.sh > "$LAB/cancel.log" 2>&1
```

用待测候选源码构建第一个二进制。生成器用旧版 schema，候选版自行在副本上迁移。每模式默认 3 轮，奇数轮 old→next，偶数轮 next→old。A 默认 20 秒，B 默认 24 秒、t≈3 秒同时注入三个 WS。可设置 `A_SECONDS`、`B_SECONDS`、`ROUNDS`；B 至少留出 12 秒暂停与后续恢复。重跑用新目录名或设置 `PREFIX`。仅运行指定组可传 `A` 或 `B`。

每个 case 保留客户 JSONL、窄窗 recorder 前后读数、runtime、系统采样与 SQLite 库，origin 中该臂覆盖的行号为 `(origin-start, origin-end]`。SSE 数组依次为类别、序号、调度滞后 ms、HTTP 状态、结果、首内容 ms、总耗时 ms、开始时刻（单调秒）。成功 WS 数组为 worker、序号、模式、结果、首内容秒、总耗时秒、客户端端口与开始时刻（单调秒）。按端口在系统采样中查对应的网关 Send-Q。WS 客户端一秒 CPU 样本在驱动 stderr。两类客户端都校验 terminal，干预客户端另校验 delta 数。A 的查询函数入口计数每次完整业务查询，原始 start/end 含线程号和 Unix 毫秒。隔离取消探针交替取消 quota/history，使用同机单调时钟对齐单个客户端取消、SQLite interrupt 和查询线程归还信号量。

源站大流逐事件间隔默认为 8 ms，设置 `DELTA_INTERVAL=0` 可复现未限速预试。`origin-health.jsonl` 保存最大调度迟到 ms。第一轮 smallinput/biginput 在结束后继续采集 30 秒空闲 RSS。

单独复测首轮四种大输出干预、同时读取准入槽计数，可运行 `PREFIX=slots- ROUNDS=1 B_MODES="fast slow pause stop" bash scripts/bench/run.sh B`。每臂 before/after-runtime.json 记录活动会话与 inflight，before/after-memory.json 的 now 部分另可核对握手或已连接的 WS 准入槽和 WS 字节。
