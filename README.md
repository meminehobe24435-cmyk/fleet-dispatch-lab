# fleet-dispatch-lab

**一个事件驱动的车队任务调度与实时状态平台** —— 把「任务下发 → 车辆状态上报 → 调度分配 → 交通管控 → 异常处理 → 恢复」做成一条可复现的实时链路，并把分布式/实时系统里那些"典型坑"逐个用代码和测试讲清楚。

主实现**零第三方依赖**（只用 Python 标准库）；matplotlib / redis / fakeredis 只用于报告图表和 Redis 适配层测试，缺失时自动跳过。

---

## ⚠️ 先说清楚这是什么

1. **全部数据是仿真生成的。** 车辆位置、任务、优先级、截止时间都来自固定种子的伪随机数发生器和一个手画的六枢纽场地网格。**这不是任何真实港口、码头或车队的运营数据**，也不是它们的近似。
2. **没有连接任何真实的 Redis / Kafka / RabbitMQ / MQTT 服务。** 消息总线（topic/分区/offset/消费者组/ack/nack/DLQ/背压）和状态缓存（TTL/INCR/HSET）都是本项目自实现的。Redis 适配层用 **fakeredis** 验证命令语义，fakeredis 是 Redis 命令集的内存实现，**不是 Redis 服务器**。
3. **无生产部署。** 没有容器、没有编排、没有多机部署、没有持久化存储后端、没有鉴权。HTTP 服务是单进程线程模型，只适合本地演示。
4. **数值只在同一台机器上可复现。** `py -3.12 -m fleetlab verify-repro` 保证"同一条命令跑两次，除耗时外逐字节一致"，CI 在 Ubuntu 和 Windows 上分别校验。**没有声称跨操作系统逐字节一致**。
5. **不是调度算法研究。** 调度器是**可解释的贪心 + 加权打分**（见 `fleetlab/scheduler.py` 的 `WEIGHTS`），不是最优解求解器。它证明的是链路正确性和可观测性，不是调度质量的下界。
6. **仿真时间 ≠ 真实时间。** 所有延迟指标基于**虚拟时钟**（1 tick = 1 仿真秒），真实墙钟耗时只写在 `reports/timing.json` 里。

---

## 为什么做这个

港口自动驾驶平台开发工程师的日常，是把一堆"单独看都对"的东西接成一条链路：任务要下发、车辆要上报、调度要选车、路口要互斥、指令要幂等、消息会重复会乱序会丢、进程会崩、缓存会过期、服务会超时。

这些东西的难点从来不在单个模块，而在**接缝处**：

- 事件日志和实时状态谁说了算？
- 消息重复投递了 3 次，业务副作用发生了几次？
- 操作员双击了一下"暂停"，车会不会暂停两次？
- 消费者在第 400 条消息时崩了，从哪个 offset 恢复？会不会丢？会不会重？
- 注入 155 次重复、88 次丢失、228 次乱序、1 次崩溃之后，系统最终状态和事件日志还对得上吗？

这个项目就是把这些接缝**做出来、跑起来、测出来**，并且**给出可复算的判据**，而不是"跑通了就算过"。

---

## 快速开始

```bash
git clone https://github.com/meminehobe24435-cmyk/fleet-dispatch-lab.git
cd fleet-dispatch-lab

# 平台本身零依赖，可直接运行
py -3.12 -m fleetlab demo --out reports

# 完整测试（需要 dev 依赖）
py -3.12 -m pip install -r requirements-dev.txt
py -3.12 -m pytest tests -q

# 两次运行逐字节一致性校验
py -3.12 -m fleetlab verify-repro --out reports

# 启动 HTTP API + SSE 实时推送
py -3.12 -m fleetlab serve --port 8787
```

其它子命令：`fleetlab scenarios`（列出场景套件）、`fleetlab fsm`（打印状态转移表）、`fleetlab replay`（校验事件日志重放一致性）。

---

## 关键结果

**全部数字来自本机真实运行输出**（`reports/metrics.json` 的 `headline`），可以用上面的命令重跑复现。

### 仿真规模与完成率

| 场景 | 车辆 | 任务 | tick | 完成 | 完成率 | 让行冲突 | 积压峰值 |
|---|---:|---:|---:|---:|---:|---:|---:|
| `baseline` | 12 | 50 | 690 | 48 | **0.960** | 97 | 1 |
| `overload` | 6 | 70 | 871 | 32 | 0.457 | 61 | **10** |
| `fault-injection` | 10 | 50 | 600 | 45 | 0.900 | 107 | 3 |
| `deadlock` | 10 | 50 | 952 | 37 | 0.740 | **216** | 2 |
| `cache-expiry` | 12 | 50 | 251 | 50 | 1.000 | 30 | 1 |
| `offline-recovery` | 12 | 50 | 248 | 50 | 1.000 | 31 | 1 |

> `overload` 是**故意超配**的场景（车队减半、到达率翻倍、再加一次 30 任务突发、消费端每 tick 只允许处理 6 条）。0.457 的完成率是过载的真实结果：32 个完成任务之外有 25 次等待超时退避和对应数的截止时间失败，不是链路故障。

### 调度延迟（虚拟毫秒）

| 指标 | baseline | overload |
|---|---:|---:|
| 平均调度延迟 | 17 760 ms | — |
| P50 调度延迟 | **0 ms** | 503 000 ms |
| **P95 调度延迟** | **59 000 ms** | 739 000 ms |
| P95 任务周期 | 535 000 ms | — |

P50 为 0 表示中位数的任务在创建当个 tick 就被派车；P95 反映了尾部排队。百分位用**最近秩法**从真实样本算出，不做插值 —— 插值会报出一个没有任何请求真正经历过的数。

### 消息与幂等

| 指标 | 数值 |
|---|---:|
| 发布消息总数（6 场景合计） | 2 635 |
| **积压峰值** | **10**（受背压上限 10 约束） |
| 背压拒绝发布 | 37 |
| 被背压推迟、随后成功的发布 | 37 |
| **去重命中（duplicates_suppressed）** | **110** |
| 乱序检出 | 50 |
| gap 回读恢复 | 47 |
| 死信路由 / 重投恢复 | 9 / 9 |
| 死信重投后残留 | **0** |

### 故障注入后的最终一致性结论

`fault-injection` 场景一次性注入全部六种故障：

| 故障 | 注入次数 | 系统表现 |
|---|---:|---|
| 消息重复 | **155** | 去重抑制生效，无额外副作用 |
| 消息乱序 | **228** | 消费端顺序门缓冲后按序释放 |
| 消息丢失 | **88** | 检测到 gap → 回退到已提交 offset 重读 → 任务无丢失 |
| 消费者崩溃 | **1** | 从已提交 offset 恢复，不丢不重 |
| 处理超时 | **8** | nack → 重试 → 成功 |
| 毒消息 | **4** | 耗尽重试 → 死信 → 重投恢复 |

**判定：`consistent = True`**。四个子检查全部通过：

| 检查项 | 结果 |
|---|:--:|
| `replay_matches_live`（事件日志重放指纹 == 实时状态指纹） | ✅ |
| `no_task_lost`（50 个任务全部到达终态） | ✅ |
| `no_duplicate_effect`（无任务达到终态两次） | ✅ |
| `injected_faults_absorbed`（每种已触发故障都有对应吸收计数） | ✅ |

**全部 6 个场景均为 `consistent = True`。**

### 指令幂等

同一个 `PAUSE` 指令连发 **10 次**：

- 生效 **1** 次，重复 **9** 次
- 车辆 `pause_commands` 计数变化：**1**
- 事件日志中 PAUSE 记录数：**1**
- `RESUME` 同样连发 10 次：生效 1 次，重复 9 次

通过 HTTP 复现（`scripts/smoke_api.py` 的真实输出）：

```
$ curl -s -X POST /api/commands -d '{"action":"PAUSE","task_id":"API0051","command_id":"SMOKE-PAUSE-1"}'
# pause the in-flight task  -> HTTP 200
{"command": {"command_id": "SMOKE-PAUSE-1", "detail": "ok", "status": "applied"}, "state": "PAUSED", "task_id": "API0051"}

$ curl -s -X POST /api/commands -d '{"action":"PAUSE","task_id":"API0051","command_id":"SMOKE-PAUSE-1"}'
# pause again with the SAME command id -> one effect only  -> HTTP 200
{"command": {"command_id": "SMOKE-PAUSE-1", "detail": "PAUSE already applied", "status": "duplicate"}, "state": "PAUSED", "task_id": "API0051"}
```

### 死锁与异常恢复

- `deadlock` 场景（车辆在协商下一段时继续占用所在路口 = 真正的 hold-and-wait）：检出并打破 **4** 次死锁，216 次让行冲突，完成率 0.740，最终一致 ✅
- `offline-recovery` 场景：3 次心跳超时离线 → 3 条 `vehicle_offline` 告警 → 3 次重连恢复，完成率 1.000，最终一致 ✅
- `cache-expiry` 场景：热缓存被注入 **59** 个键的过期，状态从事件日志重建，业务状态零影响，完成率 1.000，最终一致 ✅

### 连跑两次一致性结论

```
$ py -3.12 -m fleetlab verify-repro --out reports
matches the committed reference digest (c7b004fc1289f5fa…)
run 1 metrics.json: 1840405 bytes
run 2 metrics.json: 1840405 bytes
byte-identical: True
REPRODUCIBLE
```

磁盘上的 `reports/metrics.json` 为 **1 840 406 字节**（LF 换行，比上面的字符串多 1 字节的结尾换行）。唯一不确定的值是墙钟耗时，单独放在 292 字节的 `timing.json` 里。

CI 在 Ubuntu 与 Windows 上各自执行这项校验，并额外比对仓库中已提交的基准摘要 `reports/metrics.committed.sha256`：

```
c7b004fc1289f5fa421001170a4f9d574256f2c50ca14cb18a6dc8dfe667861e  metrics.json
```

摘要是对**把 CRLF 归一化为 LF 之后**的字节计算 `sha256` 的 —— 因为 Git 在 Windows 上可能无论如何都按 CRLF 存储文本，用文件原始字节做比较会让两个 runner 里的某一个因为换行符而不是因为数字失败。归一化之后，比较的是真正重要的东西：**同样的数字、同样的顺序、同样的格式**。

`scripts/check_readme.py` 则从**重新生成的** `metrics.json` 反查本 README 里引用的每一个数字、测试名和文件路径，任何一处对不上就让 CI 失败 —— 换句话说，「不编造数字」在这里是一条**被 CI 强制的性质**，不是一句承诺。

### 测试数量

```
tests/test_task_fsm.py           63
tests/test_cache.py              41
tests/test_bus.py                40
tests/test_observability.py      42
tests/test_httpd.py              50
tests/test_demo_integration.py   43
tests/test_replay_consistency.py 37
tests/test_redis_adapter.py      30
tests/test_faults.py             29
tests/test_scheduler.py          26
tests/test_traffic.py            25
tests/test_idempotency.py        21
------------------------------------
合计                            447   （全部通过；要求 ≥70）
```

其中**注入已知故障并断言系统正确恢复**的测试至少 6 项（不含故障注入器本身的单元测试）：

| 测试名 | 注入的故障 | 断言 |
|---|---|---|
| `test_bus.py::test_consumer_suppresses_a_duplicate_delivery` | 同 msg_id 重复投递 | 业务副作用只有 1 次 |
| `test_bus.py::test_out_of_order_delivery_is_buffered_and_released_in_order` | 批次内乱序 | 应用顺序仍为 0..4 |
| `test_bus.py::test_dropped_delivery_is_recovered_by_reseeking_to_the_commit` | 丢失一条投递 | 6 条全部处理，无重复 |
| `test_bus.py::test_crash_then_recover_loses_nothing_and_duplicates_nothing` | 消费端崩溃 | 不丢不重 |
| `test_replay_consistency.py::test_all_faults_together_still_converge` | 六种故障同时 | 最终一致，DLQ 清空 |
| `test_replay_consistency.py::test_ten_identical_pause_commands_apply_once` | 指令重复 10 次 | 只生效 1 次 |
| `test_replay_consistency.py::test_message_loss_is_recovered_by_gap_rereading` | 12% 丢包 | gap 回读恢复 |
| `test_replay_consistency.py::test_losing_the_cache_costs_a_rebuild_not_correctness` | 缓存全量过期 | 状态指纹不变 |

---

## 「典型问题 → 本项目的做法」对照表

| # | 典型问题 | 本项目的做法 | 代码位置 | 关键测试 |
|---|---|---|---|---|
| 1 | **服务间状态一致性**：谁说了算？重放对不对得上？ | 事件日志是唯一事实来源；**所有状态变更都走同一个 applier**；一致性判据是 `PlatformState.lifecycle_fingerprint()`（任务状态/指派/车辆生命周期的规范投影 SHA-256）。热缓存是**可丢弃的投影**，丢了只花一次重建。 | `eventlog.py`、`runtime.py::apply`、`state.py::lifecycle_fingerprint` | `test_replay_reproduces_the_lifecycle_fingerprint`、`test_the_replay_check_has_teeth`（负向对照）、`test_losing_the_cache_costs_a_rebuild_not_correctness`、`test_snapshot_and_restore_reproduce_the_full_state` |
| 2 | **消息重复 / 乱序 / 丢失** | 分区保留同键顺序；消费端顺序门（检测到空洞先缓冲，窗口溢出则失败开放并计数）；丢失由"已提交 offset 落后于读游标"检出后回退重读；重复按稳定 `msg_id` 两阶段 claim 去重。 | `bus.py::Consumer._gate`、`Consumer.recover_gap`、`idempotency.py` | `test_consumer_suppresses_a_duplicate_delivery`、`test_out_of_order_delivery_is_buffered_and_released_in_order`、`test_dropped_delivery_is_recovered_by_reseeking_to_the_commit`、`test_order_window_overflow_forces_a_release`、`test_message_loss_is_recovered_by_gap_rereading`（场景级） |
| 3 | **接口超时与重试** | 处理器失败在**写日志之前**抛出，保证可安全重试；nack → 有界重试 → 毒消息进死信 → DLQ 重投（**复用原 msg_id**，否则重复投递会绕过去重产生重复日志）。生产者侧背压 fail-fast，被拒的发布按 subject FIFO 延后重试，避免同任务事件被后发的抢跑。 | `bus.py::ConsumerGroup.nack`、`runtime.py::redrive_dlq`、`sim.py::_publish` | `test_consumer_retries_a_failed_handler_and_succeeds`、`test_consumer_dead_letters_a_permanently_failing_message`、`test_timeouts_are_retried_and_do_not_reach_the_dlq`、`test_dlq_experiment_recovers_by_redrive`、`test_backpressure_refuses_a_publish_and_counts_it` |
| 4 | **指令幂等和去重** | `IdempotencyStore` 用**两阶段 claim**（claim → mark / release），而不是一次性 seen 集合 —— 一次性集合会让去重把重试也一起屏蔽掉，消息静默丢失。指令层再加 `verify` 回调：生效没被观测到就释放 key，允许重试。 | `idempotency.py`、`runtime.py::issue_command` | `test_release_allows_a_retry_to_run`（去重与重试的兼容性）、`test_ten_identical_pause_commands_apply_once`、`test_verify_releases_the_key_when_the_effect_never_landed`、`test_pause_command_is_idempotent_over_http`、`test_a_failing_command_can_be_retried` |
| 5 | **服务异常与故障恢复** | 消费端 `cursor`（推测读位点）与 `committed`（连续 ack 前缀）分离，崩溃只丢推测位点；车辆心跳超时判离线并**记入事件日志**（否则重放对不上）；交通层检出 wait-for 环并打破；等待超时则**退避**（只放弃排队位置和车道，不放弃已装载的货）；系统级死锁兜底 + 截止时间失败保证收敛。 | `bus.py::ConsumerGroup.crash/recover`、`runtime.py::check_timeouts`、`traffic.py::detect_deadlock/break_deadlock`、`sim.py::_abandon_wait` | `test_crash_then_recover_loses_nothing_and_duplicates_nothing`、`test_vehicle_going_offline_is_journaled_and_replayable`、`test_three_way_cycle_is_detected_and_broken`、`test_deadlock_victim_is_the_lowest_priority_vehicle`、`test_all_faults_together_still_converge` |
| 6 | **实时状态与历史事件的职责划分** | 明确分三层：**事件日志**（只追加、全局有序、事实来源，永不截断）／**实时状态表**（原地覆盖，回答"车现在在哪"）／**热缓存**（带 TTL，回答"当前投影"，可丢弃可重建）。在线与否**不是**状态机里的状态 —— 一辆车可以在任何状态下离线，混为一谈就会把任务派给断链的车。 | `eventlog.py`、`state.py`、`cache.py`、`fleet.py`（`online` 与 FSM state 分离）、`runtime.py::cache_*` | `test_full_fingerprint_includes_pose_but_lifecycle_does_not`、`test_cache_projection_matches_the_state_of_record`、`test_vehicle_offline_and_back_online`、`test_offline_vehicle_raises_an_alert`、`test_a_heartbeating_vehicle_is_never_taken_offline` |

---

## 已实现的范围

### 1. 任务生命周期状态机（`task_fsm.py`）
10 个任务状态（`PENDING / ASSIGNED / ENROUTE / QUEUED / EXECUTING / PAUSED / RECOVERING / COMPLETED / FAILED / CANCELLED`）+ 11 个车辆状态、34 条任务迁移边。事件驱动，非法迁移一律**拒绝并记录原因**（`RejectionRecord` 带 `from / event / reason`），状态保证不变。两个**动态边**：`RESUME` 和 `PROCEED` 的目标由栈决定（暂停/排队时从哪来就回哪去）。输出状态转移表与状态图 PNG。

### 2. 调度与交通管控（`scheduler.py`、`traffic.py`、`fleet.py`）
`Scheduler.decide()` 是**纯函数**（只读状态、只返回决策，不写），每个决策带可解释的打分明细。支持优先级抢占（被抢占任务 REQUEUE 回队列，不是丢弃）。交通层三类互斥资源：路口（容量 1）、单车道（容量 1，分方向）、泊位（容量 >1）。让行规则是**全序**的（优先级降序 → 到达时间升序 → 车辆 ID 升序），wait-for 图上的环由迭代式 DFS 检出，按"环内优先级最低、ID 最大"打破。车辆实时状态含位置/电量/载货/在线，心跳超时判离线，低电自动充电，故障自动维修。

### 3. 实时状态缓存（`cache.py`、`redis_adapter.py`）
自实现 `TTLStore`，语义对齐 Redis：`SET EX / GET / DEL / EXPIRE / TTL / PERSIST / INCR / HSET / HGET / HGETALL / HDEL / KEYS`。TTL 返回 `-2`（不存在）/ `-1`（无过期）/ 正数（向上取整）。`WRONGTYPE` 映射到 `StoreTypeError`。线程安全（`RLock`），快照/恢复**跨后端格式统一**。`RedisStateStore` 在 redis-py 之上实现同一套协议，同一段操作序列在两个后端上结果完全一致（`test_conformance_sequence_agrees_between_backends`）。

### 4. 消息总线（`bus.py`）
topic / 分区（MD5 稳定哈希路由）/ offset / 消费者组 / ack / nack / 重试 / 死信队列 / 背压 / 分区内有序 / 乱序检测与重排 / 按 `msg_id` 去重 / 事件重放。

### 5. 故障注入与一致性验证（`faults.py`、`demo.py::verify_scenario`）
六种可开关故障：消息重复、消息乱序、消息丢失、消费者崩溃重启、缓存过期、网络超时重试（另有毒消息）。每次注入都计数，测试断言**故障确实触发过**。一致性用四个独立检查判定。

### 6. 可观测性（`observability.py`）
结构化 JSON 日志（`trace_id` / `span_id` / 事件类型 / 耗时 / 虚拟时间）、指标（计数/仪表/直方图，P95 用**最近秩法**从真实样本计算，退化输入有定义）、链路追踪（一个 `trace_id` 串起 `command.issue → bus.publish → consumer.process → 状态变更 → command.response`，并提供按 `trace_id` 查询）、告警规则（积压超阈值、车辆离线、任务超时、死信非空，带冷却期防刷屏）。

### 7. 接口层（`httpd.py`）
零依赖自实现 HTTP/1.1 路由 + REST API + SSE 实时推送：

```
GET    /healthz                GET  /api/tasks[?state=]      GET  /api/tasks/{id}
POST   /api/tasks              DELETE /api/tasks/{id}        GET  /api/vehicles[/{id}]
POST   /api/commands           GET  /api/traces/{trace_id}   GET  /api/metrics
GET    /api/alerts             GET  /api/bus                 GET  /api/events (SSE)
```

路由按注册顺序匹配（静态优先于参数化），`HEAD` 回落到 `GET`（RFC 9110），405 带 `Allow` 头，SSE 客户端队列有界（慢客户端被丢弃而不是把服务端撑爆）。`scripts/smoke_api.py` 用真实 socket 调用全部端点，README 里的响应片段是它的真实输出。

### 8. 工程实践（`cli.py`、`demo.py`、`plots.py`）
`py -3.12 -m fleetlab demo --out reports` 一条命令跑完整演示，产出 `metrics.json`、`实验报告.md`、`timing.json`、`alerts.csv`、`events.jsonl` 和 5 张 PNG。CI 双平台跑测试 + 演示 + 可复现性比对。

---

## 开发过程中被测试抓出来的真问题

这些都是**先写错、被测试或仿真指标抓出来**的真实问题，不是事后补写的漂亮话：

1. **现象**：车辆状态机里 `QUEUED / ENROUTE / EXECUTING` 没有 `RELEASE` 边，任务结束时车辆收不到释放事件，一路 `QUEUED` 到运行结束，完成率掉到 **0.275**，看起来像交通拥堵。
   **修法**：给所有"持有任务"的车辆状态补上 `RELEASE`，并加回归测试 `test_every_task_holding_vehicle_state_accepts_release`。教训：**无法被释放的状态 = 永久损失的车**。

2. **现象**：`cache-expiry` 场景报告"已标记 59 个键过期"，但缓存里一个键都没丢，场景却仍然判定 `consistent = True` —— 一个什么都没测的故障测试。
   **修法**：根因是 Windows 上 `time.monotonic()` 分辨率约 **15.6 ms**，1 µs 的 TTL 永远等不到过期。新增 `TTLStore.expire_now()` 走确定性的强制过期路径，并把 TTL 边界测试改为注入假时钟。教训：**用挂钟测亚毫秒 TTL 是在测时钟分辨率**。

3. **现象**：给死信队列加重投后，事件的 `task_created` 记录在日志里出现了 **52** 条而任务只有 50 个，`no_task_lost` 失败，但业务状态完全正确 —— 一个只在审计轨迹里现形的 bug。
   **修法**：重投时**复用原始 `msg_id`**，让去重器仍然认得这条消息。原先"反正没应用到，换个新 ID 更安全"的想法是错的：新 ID 让这条消息对去重器隐形，一条**已经应用过**的消息会被再应用一次。

4. **现象**：第一次跑仿真只完成 5/40 个任务就跑到 tick 上限；车辆状态分布显示 7 辆车卡在 `QUEUED`。
   **修法**：一路查下来是"在拿到车位之前就先发 `ENQUEUE`"导致任务进入 `QUEUED`，而 `DISPATCH` 在 `QUEUED` 下不合法，任务再也回不到 `ASSIGNED`；同时 `_abandon_wait` 直接把整单退回重排队，已经装上货的车又跑回取货点。改成 `DISPATCH` 在建单时就发出、退避只放弃排队位置和车道。教训：**非法迁移被正确拒绝，是暴露了上游的时序错误，而不是拒绝本身有问题**。

5. **现象**：`verify-repro` 报告两次运行"几乎一样"，唯一的差异是 `summary.seconds`；随后又发现磁盘上的 `metrics.json` 比内存里的字节数**大 119 059 字节**。
   **修法**：第一个是把墙钟耗时从确定性指标里彻底移出（只留在 `timing.json`）；第二个是 Windows 文本模式把每个 `\n` 写成了 `\r\n`，改为二进制写入并显式使用 LF。教训：**"逐字节一致"要先确认比较的到底是同一串字节**。

6. **现象**：`timeouts` 为 6 的测试里，消息进了死信队列而不是被重试成功。
   **修法**：重试是**优先于新消息**投递的，所以同一条消息会连续吃掉整个超时预算，超过 `max_retries` 就合理地进死信。测试里把 `max_retries` 提到预算之上以专门验证重试路径，死信路径由另一个测试覆盖。教训：**"重试"和"死信"是两个需要分别构造的场景**。

7. **现象**：第一次跑出 2599 个"让行冲突"，但吞吐很差；剖析发现 **1299 个车辆 tick 卡在同一条车道**（`LANE_B_E`，场地中央唯一的南北通道）。
   **修法**：把最短路替换为**拥堵感知路由**（Dijkstra，边权 = 长度 + 拥堵罚项），并把车道容量从"互斥 1"调整为"跟车 2"、泊位容量从 1 调整为 ≥2（车辆数 8 > 枢纽数 6 时，容量 1 的泊位会造成结构性饥饿）。教训：**先剖析再调参；单车道热点不是调度能解决的问题**。

8. **现象**：`no_task_lost` 在故障场景下间歇性失败，报错显示 5 个任务在运行结束时仍是 `PENDING`。
   **修法**：运行循环判定"全部完成"时，死信队列里还躺着 7 条 `task_created`；这些事件从未被应用，所以它们描述的任务当时根本不存在，而重投发生在循环之后。把 DLQ 重投挪进 tick 循环（真实部署里它本来就是个定时任务），并在最终重投后再给一个有界的窗口。教训：**"完成"的判据必须包含"没有还在飞的输入"**。

9. **现象**：`HEAD /healthz` 返回 405，一个再正常不过的 `curl -I` 被拒。
   **修法**：路由层让 `HEAD` 回落到 `GET` 路由。教训：**HTTP 语义的细节不是可选项**。

10. **现象**：SSE 的"客户端断开后清理订阅者"测试单独跑通过，全量跑失败 —— 服务端感知对端断开取决于操作系统和下一次写 socket 的时机。
    **修法**：把"清理逻辑"和"操作系统是否通知我们"拆开测：直接断言 `detach()` 的行为（`test_detach_removes_a_subscriber_and_tolerates_an_unknown_id`）、帧编码器的输出（`test_sse_frames_start_with_a_hello_then_stream_events`）、有界队列的丢弃策略；真实断连路径交给 `scripts/smoke_api.py` 端到端验证。教训：**测内核时序的测试是 flaky 测试**。

---

## 边界与已知限制（明确写"没做什么"）

1. **没有真实服务。** 没有 Redis / Kafka / RabbitMQ / MQTT 服务器，也就没有验证：线路协议、真实内存压力下的淘汰策略、主从复制、故障切换、真实网络超时与重连。Redis 适配层只在 **fakeredis** 上验证过命令语义。
2. **单进程、无持久化后端。** 所有状态在内存里；进程结束即丢失。HTTP 服务是 `ThreadingHTTPServer`，无鉴权、无 TLS、无限流、无请求体大小上限，**不可用于生产或公网**。
3. **一致性判据的边界。** `lifecycle_fingerprint` 只覆盖任务状态、指派和车辆生命周期；**位置和电量不在其中**（它们由物理循环产生而非事件产生）。因此"重放一致"证明的是**事件与生命周期的可重放性**，不等于"整机状态逐位可重放"。快照/恢复用更强的 `full_fingerprint` 覆盖位姿。
4. **地理位置是玩具级的。** 6 个枢纽、7 条边、直线匀速、瞬时转弯、无动力学、无避障、无挂车模型、无 GPS 噪声。真实港口的路径规划和车辆控制完全不在范围内。
5. **调度器不是最优的。** 加权贪心 + 抢占，权重是手调常量；没有形式化的最优性、稳定性或竞争比保证，也没有跟任何基线算法做对比实验。
6. **故障注入是确定性的、进程内的。** 用固定种子的 PRNG 在投递边界注入，不涉及真实网络分区、时钟漂移、磁盘故障、OOM、多进程并发消费同一分区。
7. **规模很小。** 最多 12 辆车 / 70 个任务 / 约 2 600 条消息。没有做吞吐量压测，也没有性能回归基线。所有"延迟"都是**虚拟时钟**下的仿真值。
8. **跨操作系统逐字节一致未经验证。** 为保证可移植性，距离计算用 `math.sqrt` 而非 `math.hypot`（前者由 IEEE-754 正确舍入，后者依赖 libm 实现），但本机无法验证 Linux 结果；CI 在两平台各自校验"同平台两次运行一致"。
9. **`reports/*.png` 的内容不在一致性比对范围内**（只比对 `metrics.json`）。图表由 matplotlib 版本决定细节。

---

## 目录结构

```
fleet-dispatch-lab/
├── fleetlab/
│   ├── __init__.py            版本与包说明
│   ├── __main__.py            py -3.12 -m fleetlab 入口
│   ├── task_fsm.py            任务/车辆状态机（10 + 11 状态，34 条迁移边）
│   ├── state.py               任务聚合、canonical 投影与一致性指纹
│   ├── fleet.py               车辆实时状态、心跳、离线检测
│   ├── cache.py               Redis 语义的 TTL 键值存储（内存实现）
│   ├── redis_adapter.py       同一协议在 redis-py 之上的实现
│   ├── bus.py                 topic/分区/offset/消费者组/ack/nack/DLQ/背压/去重/重放
│   ├── idempotency.py         两阶段 claim 去重 + 指令幂等
│   ├── eventlog.py            只追加事件日志（事实来源，JSONL 持久化）
│   ├── scheduler.py           纯函数调度决策、打分解释、优先级抢占
│   ├── traffic.py             路口/车道/泊位互斥、让行决策、死锁检测与打破
│   ├── faults.py              六种故障注入器 + 故障消费组代理
│   ├── observability.py       JSON 日志、指标与百分位、链路追踪、告警规则
│   ├── httpd.py               零依赖 HTTP/1.1 路由 + REST + SSE
│   ├── runtime.py             平台装配与唯一的状态 applier
│   ├── sim.py                 六枢纽场地仿真（虚拟时钟、拥堵感知路由）
│   ├── demo.py                场景套件、一致性判定、报告产出
│   ├── plots.py               状态图 / 甘特图 / 积压图 / 场景对比图
│   └── cli.py                 命令行接口与 Markdown 报告渲染
├── tests/                     447 项测试（12 个文件）
├── scripts/
│   ├── smoke_api.py           真实 socket 调用全部 HTTP 端点
│   └── check_readme.py        反查 README 里的数字/测试名/路径是否真实
├── reports/                   演示产出（含已提交的 metrics.json 基准）
├── .github/workflows/ci.yml   Ubuntu + Windows 双平台 CI
├── pyproject.toml             打包与 pytest 配置
├── requirements-dev.txt       仅测试/绘图用的可选依赖
└── LICENSE                    MIT
```

---

## 测试与 CI

```bash
py -3.12 -m pytest tests -q                    # 447 项
py -3.12 -m pytest tests -q -m redis_adapter   # 仅 Redis 适配层一致性
py -3.12 -m pytest tests/test_replay_consistency.py -q
```

CI（`.github/workflows/ci.yml`）在 **Ubuntu + Windows / Python 3.12** 上执行：

1. 安装 `.[dev]`，并**断言核心模块在没有 matplotlib / redis / fakeredis 时也能导入**（零依赖是硬约束，用测试守住）
2. 跑全部 447 项测试
3. 跑完整演示 `fleetlab demo --out reports`
4. 跑 `fleetlab verify-repro`（两次运行逐字节比对）
5. 比对重新生成的 `metrics.json` 与仓库中已提交的基准摘要 `reports/metrics.committed.sha256`
6. 跑 `fleetlab replay` 校验事件日志重放
7. 跑 `scripts/check_readme.py` 反查 README 里的每个数字/测试名/路径
8. 跑 `scripts/smoke_api.py`（真实起服务、真实调用全部端点）
9. 上传 `reports/` 作为构建产物

---

## 许可

[MIT](LICENSE) © 2025 meminehobe24435-cmyk
