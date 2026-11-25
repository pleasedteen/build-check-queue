# 本地产物校验队列（Python + SQLite，仅标准库）

一套可在单机运行的校验任务队列：使用者向**队列**提交指定**收件端**的校验
任务，多个独立**工作进程**通过租约抢占任务、用项目自带样例执行校验，队列
只接受持有当前有效租约的进程交回的结果，并由后台投递线程把结果持续送到
各项目自己的**收件服务**。所有数据保存在本地 SQLite 文件中，队列与各收件
服务各自持有数据、可独立重启，重启后已完成任务继续投递、无需重新校验。

## 架构与数据流

```
 使用者(client.py)                队列服务 queue_server.py (queue.db)
 ──submit(recipient, artifact)──▶ ┌──────────────────────────────┐
                                  │ tasks: queued→leased→         │
 工作进程 worker.py (可多个)       │        succeeded→delivered    │
   ── POST /lease （租约 TTL）──▶  │ 租约令牌校验 / 名额控制        │
   ◀─ 带 lease_token 的任务 ─────  │ 后台投递线程（失败重试）        │
   ── 子进程调用样例校验器 ──       └──────────────┬───────────────┘
   ── POST /tasks/<id>/complete ──▶               │ POST /results
                                       ┌──────────┴───────────┐
                                       ▼                      ▼
                          收件服务 inbox_server.py     inbox_server.py
                          (alpha.db, 名额=2)           (beta.db, 名额=1)
```

关键机制：

- **租约换手**：工作进程崩溃或卡住时，租约到期（`--lease-ttl`）后任务可被
  另一进程领取；每次领取换发新令牌，旧进程之后交回结果会收到 `409` 被拒绝。
- **结果只认有效租约**：`complete` 同时校验任务状态与令牌，重复/过期交回
  一律拒绝，保证一个任务只接受一份结果。
- **持久化续投**：结果一旦被接受即落盘为 `succeeded`；队列重启、收件服务
  长时间停机都不丢任务，恢复后投递线程自动续投，不需要新提交触发。
- **收件端名额**：每个收件端配置 `max_inflight`（未收妥任务数上限）。任务
  从**首次领出**起占名额，直到队列记下该端 `delivered` 回执才释放。满额时
  仅暂停该端任务的**首次领取**；新任务提交、到期换手、其他收件端均不受影响。
- **收件端幂等**：同一 `task_id` 在收件端只保留一份结果，重复投递只累加
  `duplicate_count`。

## 目录结构

```
validation_queue/
├── vq/
│   ├── common.py                 # SQLite/HTTP 公共工具
│   ├── queue_server.py           # 队列服务（租约、名额、投递循环）
│   ├── inbox_server.py           # 项目收件服务（独立 SQLite）
│   ├── worker.py                 # 工作进程（多实例可并行）
│   ├── client.py                 # 命令行客户端
│   └── samples/
│       ├── sample_validator.py   # 自带样例校验器（子进程调用）
│       ├── good_artifact.json
│       ├── bad_checksum_artifact.json
│       └── corrupted_artifact.json
├── tests/
│   └── e2e_test.py               # 端到端测试（一条命令，<80s）
└── README.md
```

## 本地启动说明

需要 Python 3.10+，无第三方依赖。以下命令均建议用 `python3 -I` 隔离运行。
打开 5 个终端（或用 `&` 后台运行）：

```bash
cd validation_queue
mkdir -p data

# 1) 队列服务（租约 10 秒，投递轮询 0.5 秒）
python3 -I vq/queue_server.py --port 8080 --db data/queue.db \
    --lease-ttl 10 --delivery-interval 0.5

# 2) 两个项目各自的收件服务（各自独立数据库）
python3 -I vq/inbox_server.py --id alpha --port 9001 --db data/alpha.db
python3 -I vq/inbox_server.py --id beta  --port 9002 --db data/beta.db

# 3) 注册收件端：alpha 未收妥上限 2，beta 上限 1
python3 -I vq/client.py register --id alpha \
    --inbox http://127.0.0.1:9001 --max-inflight 2
python3 -I vq/client.py register --id beta \
    --inbox http://127.0.0.1:9002 --max-inflight 1

# 4) 启动两个独立工作进程（共享同一队列，可启动任意多个）
python3 -I vq/worker.py --queue http://127.0.0.1:8080 \
    --validator "$PWD/vq/samples/sample_validator.py" --worker-id worker-1
python3 -I vq/worker.py --queue http://127.0.0.1:8080 \
    --validator "$PWD/vq/samples/sample_validator.py" --worker-id worker-2

# 5) 提交任务（artifact 为绝对路径，工作进程按路径读取产物）
python3 -I vq/client.py submit --recipient alpha \
    --artifact "$PWD/vq/samples/good_artifact.json"
python3 -I vq/client.py submit --recipient beta \
    --artifact "$PWD/vq/samples/corrupted_artifact.json"

# 6) 查询进度 / 在收件端查看结果
python3 -I vq/client.py status                       # 全部任务
python3 -I vq/client.py status --recipient alpha
python3 -I vq/client.py status --task-id <任务id>
python3 -I vq/client.py recipients                   # 查看名额占用
python3 -I vq/client.py inbox --inbox http://127.0.0.1:9001
python3 -I vq/client.py inbox --inbox http://127.0.0.1:9001 --task-id <任务id>
```

### 验证故障与重启（手动）

```bash
# 工作进程卡住/被杀：租约 10s 到期后，另一个 worker 自动接手
kill -9 <worker-1 的 pid>

# 收件服务长时间停机：任务结果留在队列（succeeded）持续重试
kill <inbox beta 的 pid>
# ... 恢复后无需任何新提交，投递线程自动续投
python3 -I vq/inbox_server.py --id beta --port 9002 --db data/beta.db

# 队列自身重启：已接受结果不丢、不重跑校验，启动后继续投递
kill <queue 的 pid>
python3 -I vq/queue_server.py --port 8080 --db data/queue.db --lease-ttl 10
```

## 测试命令

一条命令自动拉起队列、3 个收件服务和多个工作进程，覆盖正常流程、租约
换手（含过期租约 409 拒绝）、名额满额隔离、收件端/队列重启恢复与幂等
去重，约 30~50 秒输出可读结果（要求 80 秒内）：

```bash
python3 -I tests/e2e_test.py
```

输出形如 `[ 12.34s] ✅ ...` 的逐项断言，末尾给出通过/失败统计；退出码
0 表示全部通过。临时数据库与进程日志放在 `/tmp/vq-e2e-*` 目录（结束时
打印路径），失败时可直接查看各进程日志定位。

## HTTP API 摘要

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/recipients` | 注册/更新收件端 `{id, base_url, max_inflight}` |
| GET  | `/recipients` | 收件端列表及当前名额占用 `inflight` |
| POST | `/tasks` | 提交任务 `{recipient, artifact}` |
| GET  | `/tasks` / `/task?id=` | 任务列表 / 单任务进度 |
| POST | `/lease` | 工作进程领取任务（返回 `lease_token` 与到期时间） |
| POST | `/tasks/<id>/complete` | 凭 `lease_token` 交回结果，过期/重复返回 409 |
| POST | 收件端 `/results` | 队列投递结果；收件端按 `task_id` 幂等去重 |
| GET  | 收件端 `/results[?task_id=]` | 在收件端查询结果 |
