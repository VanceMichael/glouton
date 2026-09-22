# MQTT 持久待发队列（Spool）- 产品需求文档

## Overview
- **Summary**: 为 Glouton 的两条 MQTT 发送链路——开放 MQTT（`mqtt` 包，配置节 `mqtt`）与 Bleemeo MQTT（`bleemeo/internal/mqtt`，配置节 `bleemeo.mqtt`）——增加一个**可选的、受 agent 状态目录管理的磁盘持久待发队列**。消息在收到 broker 的 MQTT PUBACK 之前一直保留；进程在"已发布、未确认"窗口内退出后，下次启动按 QoS 1（至少一次）语义重放，可能重复，不承诺不重复。
- **Purpose**: 监控主机到 broker 的连接中断、且 Glouton 需要重启（升级、崩溃、被杀）时，已经被 agent 接收并完成编码的指标不能在进程退出时静默消失；同时所有丢弃/降级路径必须可观察。
- **Target Users**: Glouton 运维人员（配置持久队列、读取诊断归档定位丢失/淘汰原因）；Glouton 开发者（复用统一的发送确认层）。

## Goals
- broker 确认（QoS 1 PUBACK）是消息退休的**唯一**正常条件；确认前崩溃可恢复重放。
- 持久化默认关闭；关闭时行为与当前纯内存实现完全一致（1000 条内存 FIFO、满则丢弃并记日志）。
- 容量（字节上限）与保留期限（最长年龄）达到上限时按**预先确定的固定规则**淘汰，且每次淘汰都可计数、可在诊断中观察。
- 队列文件尾部截断或损坏时恢复有效前缀；满盘、编码失败、无法恢复的记录三类故障分别计数并暴露给诊断。
- reload（SIGHUP/文件监听触发的进程内热重载）与跨进程重启都只有**一个回放所有者**。
- 开放 MQTT 的批量取点、Bleemeo 的注册过滤、失败点 1 小时年龄、暂停发送/暂停缓冲与背压语义全部保持不变。

## Non-Goals
- 不实现 QoS 2 / exactly-once，不做去重（broker 或下游必须容忍 QoS 1 重复）。
- 不持久化 `retry=false` 的控制消息（`ping`、`top_info`、`disconnect`）：它们是即发即弃消息，崩溃后重放会产生语义错误的陈旧消息；持久队列只覆盖 `retry=true` 的消息（指标批量、日志、connect）。
- 不改造 Bleemeo `pendingPoints`/`failedPointsCache`（尚未编码成 MQTT 消息的点）与开放 MQTT 的 `pendingPoints`：它们维持现有 reload/内存语义。
- 不持久化 API、Prometheus remote write 等非 MQTT 输出。
- 不做静态加密、压缩（磁盘载荷沿用现有 zlib 压缩后的 MQTT payload，原样落盘）。
- 不引入外部 broker 协调或跨 agent 去重。

## Background & Context

### 现状（代码证据）
- 两条链路共用底层客户端 [client.go](file:///Users/vance/project/mineProject/swe/092112/project-01/mqtt/client/client.go)：
  - `publishWrapper()` 先 `publish()`（调用 `paho.Client.Publish(topic, 1, false, payload)`，QoS 1）拿到 `Token`，再 `ReloadState.AddPendingMessage(msg, true)` 进入内存 FIFO。
  - `ackManager()` goroutine 从 FIFO 取消息，`ackOne()` 等 PUBACK；失败且 `Retry=true` 时重新 `Publish` 并重新入队；成功后回收 payload buffer——消息即退休。
  - FIFO 是固定容量 1000 的环形队列 [fifo.go](file:///Users/vance/project/mineProject/swe/092112/project-01/mqtt/client/fifo.go)；`PutNoWait` 满时返回 false，消息被丢弃并记日志。
- 内存待发状态保存在 [state.go](file:///Users/vance/project/mineProject/swe/092112/project-01/mqtt/client/state.go) 的 `client.ReloadState`：
  - 开放 MQTT：在 [reloader.go](file:///Users/vance/project/mineProject/swe/092112/project-01/agent/reloader.go#L240-L243) 中进程级创建一次，reload 间复用，仅进程退出时 `Close()`，退出时残留消息只记日志后放弃。
  - Bleemeo：[bleemeo/internal/mqtt/state.go](file:///Users/vance/project/mineProject/swe/092112/project-01/bleemeo/internal/mqtt/state.go) 包了一层 `reloadState`，内部持有一个 `client.NewReloadState()`（`ClientState()`），首次运行时惰性创建并存入 bleemeo 的进程级 ReloadState。
- reload 是进程内热重启：旧 run `cancel()` + `wg.Wait()` 完全退出后，新 run 才启动（[reloader.go](file:///Users/vance/project/mineProject/swe/092112/project-01/agent/reloader.go#L320-L344)）；paho 连接通过 ReloadState 跨 reload 保留。因此进程内同一时刻只有一个 `ackManager`（天然单所有者屏障）。
- 开放 MQTT 每 10s `PopPoints()` 并按 `pointsBatchSize=1000` 分批发布（[mqtt.go](file:///Users/vance/project/mineProject/swe/092112/project-01/mqtt/mqtt.go#L158-L180)）。
- Bleemeo 的发送语义集中在 [bleemeo/internal/mqtt/mqtt.go](file:///Users/vance/project/mineProject/swe/092112/project-01/bleemeo/internal/mqtt/mqtt.go)：`filterPoints`/`preparePoints` 做注册过滤（未注册/停用指标进入 `failedPointsCache`）、失败点 1h 年龄（`failedPointsMaxAge`）、`SuspendSending`（只读模式）、`SuspendBuffering`（账户挂起）、`dataAckBackPressureDelay` 背压、`PendingMessagesCount() > 50` 的日志背压。
- 状态目录：`config.Agent.StateDirectory`（默认即 state.json 所在目录），state.json 采用"临时文件 + rename"原子写、0600 权限（[agent/state/state.go](file:///Users/vance/project/mineProject/swe/092112/project-01/agent/state/state.go)）。
- 诊断归档：两条链路都委托 `client.Client.DiagnosticArchive()`，产出 `<id>-mqtt-stats.txt` 与 `<id>-mqtt-client.json`（id 分别为 `open-source`、`bleemeo`）。
- 依赖中已有跨平台文件锁库 `github.com/gofrs/flock v0.13.0`（当前为间接依赖）；Go 版本 1.26，测试可使用 `testing/synctest`。

### 关键设计决策（实现约束）
1. **落盘先于发布**：spool DATA 记录 append + fsync 成功后才调用 `paho.Publish`。因此"broker 已收到但磁盘无记录"的窗口不存在；存在的窗口是"已发布未 PUBACK"与"已 PUBACK 但 RETIRE 未 fsync"，二者重放均为合法 QoS 1 重复。
2. **退休记录（RETIRE 帧）也写入 WAL**：收到 PUBACK 后追加一条 RETIRE 帧并 fsync，消息才算退休。崩溃恢复时 DATA 被对应 RETIRE 抵消者不重放；未及写入 RETIRE 的已确认消息可能重放（重复，不承诺避免）。
3. **文件格式为带魔数、版本、长度、CRC32 的分帧 WAL**，顺序扫描；首个坏帧即视为尾部损坏，恢复其前的有效前缀并截断尾部；DATA 帧 CRC 合法但载荷无法解码的单条记录计入"无法恢复的记录"并跳过，继续扫描。
4. **淘汰规则（确定性）**：
   - 大小上限按"存活（未退休）记录字节数"计量；入队空间不足时，从最老记录开始淘汰，直到新记录可放入；被淘汰记录若已在内存 FIFO 中，由 `ackManager` 出队时按 seq 状态跳过发送并计数（淘汰对发送层幂等）。
   - 年龄上限按记录入队时间戳判定；`ackManager` 出队时超龄即退休（不发送）并计数；打开 spool 恢复时超龄记录同样剔除。
   - 单条记录大于容量上限：拒绝入队，计入容量淘汰/拒绝类计数，不拆条。
5. **满盘降级而非丢消息**：append 失败且为 ENOSPC 时，消息仍按当前内存路径尝试发布，仅丧失崩溃恢复能力，`disk_full` 计数与日志独立暴露；其他写错误按不可用处理（同降级路径，单独记 `write_error`）。
6. **压缩（compaction）**：死记录比例/文件大小达阈值时，在写锁内用"临时文件 + fsync + rename + fsync 目录"重写存活记录；压缩只回收空间，不改变顺序与 seq。
7. **reload 开关语义**：spool 对象依附长寿命 ReloadState，进程内唯一；每次 run 应用配置：
   - 启用（含 reload 中由关变开）：获取文件锁、打开/恢复、把恢复记录注入内存 FIFO（一次性回放），并把当前 FIFO 中未落盘的 retry 消息补登到 spool（两类消息集合按构造互斥）。
   - 由开变关：进入 **draining** 模式——不再接受新落盘，已有 spool 记录继续由同一个 `ackManager` 回放直到排空，排空后删除文件；诊断中显示 `enabled=false, draining=true`。绝不因关闭配置而批量丢弃未确认记录。
8. **跨进程互斥**：每个 spool 一个 `gofrs/flock` 文件锁；第二个持有者获取失败时，spool 不可用并在诊断中明确报"被另一进程锁定"，降级为纯内存，绝不双写。
9. **文件位置**：`<StateDirectory>/mqtt-spool/open-source.wal(+.lock)` 与 `<StateDirectory>/mqtt-spool/bleemeo.wal(+.lock)`，目录 0700、文件 0600。
10. 配置形态（两条链路各自独立）：
    ```yaml
    mqtt:
      spool:
        enable: false
        max_size_mb: 100
        max_age: "168h"
    bleemeo:
      mqtt:
        spool:
          enable: false
          max_size_mb: 100
          max_age: "168h"
    ```
	默认关闭、100 MiB、168h（7 天）。`max_age=0` 表示不按年龄淘汰；`max_size_mb` 必须为正。

## Functional Requirements
- **FR-1（配置）**：`config.OpenSourceMQTT` 与 `config.BleemeoMQTT` 各新增 `Spool` 配置节（enable/max_size_mb/max_age），默认值如上；配置错误（如 max_size_mb<=0、非法 duration）按现有配置加载错误/告警机制暴露，agent 不因错误配置而启动失败于内存路径之外的语义。
- **FR-2（发送顺序）**：启用 spool 时，每条 `retry=true` 消息在 paho Publish 之前完成 DATA 帧持久化；`retry=false` 消息不落盘；未启用时完全走现有内存路径。
- **FR-3（确认退休）**：只有观察到该消息的 PUBACK（paho Token 成功完成）才写 RETIRE；RETIRE fsync 成功前消息保持存活。
- **FR-4（重放）**：spool 打开时扫描 WAL，存活记录按 seq 顺序各注入一次内存 FIFO（Token=nil、Retry=true），由当次 run 的唯一 `ackManager` 重新发布。
- **FR-5（单所有者）**：进程内 spool 实例与回放注入在整个进程生命周期唯一；reload 不产生第二个 ackManager/第二次恢复注入；跨进程由 flock 拒绝第二持有者。
- **FR-6（容量淘汰）**：按设计决策 4 的固定规则执行大小/年龄淘汰；每次淘汰累加分类计数器；淘汰行为对 broker 侧表现为消息不再发送（明确淘汰才允许丢弃）。
- **FR-7（损坏恢复）**：打开时从截断/CRC 错误尾部恢复有效前缀，截断落盘；无法解码的记录单条跳过并计数；不可识别的文件版本隔离（quarantine）为新文件后全新开始，计入 unrecoverable。
- **FR-8（故障分类）**：磁盘满（ENOSPC）、载荷/帧编码失败、无法恢复记录分别有独立计数器与日志字段。
- **FR-9（诊断）**：在两条链路的诊断归档中新增 spool 状态文件，含启用/排空状态、路径、存活/在飞/文件字节数、最老记录年龄、各类计数器、最近一次恢复与压缩信息。
- **FR-10（语义保持）**：开放 MQTT 批量取点（10s tick、1000/批）不变；Bleemeo 注册过滤、失败点缓存（100k 上限、1h 年龄、cleanup）、SuspendSending/SuspendBuffering、ack 背压与日志 50 条阈值（计数须包含 spool 恢复注入的消息）行为不变。
- **FR-11（关闭等价）**：配置关闭时不创建任何 spool 文件/目录/锁，内存 FIFO 容量、阻塞/非阻塞入队、退出丢弃行为与现状逐字一致。
- **FR-12（生命周期）**：reload 时 spool 保持打开、FIFO 保留；干净退出时 flush/sync 后释放锁（不等待未完成 PUBACK 超出现有 Disconnect(5s) 语义）；下次启动按 WAL+RETIRE 内容恢复。

## Non-Functional Requirements
- **NFR-1（性能）**：消息级 fsync 频率与发布频率一致（指标为每 10s 若干批量消息，量级低）；压缩在写锁内完成但单次有界，不得阻塞采集链路；诊断读取为 O(计数器)。
- **NFR-2（可移植性）**：通过 gofrs/flock 与标准库保证 Linux/macOS/Windows 均可编译运行；不使用平台特定 syscall。
- **NFR-3（安全）**：spool 目录 0700、WAL 与锁文件 0600；载荷中可能含日志等用户数据，文件权限不弱于 state.json。
- **NFR-4（可测性）**：核心 spool 逻辑不依赖真实 broker 即可单测；故障注入（坏盘/坏帧/超限）通过接口或临时目录实现。
- **NFR-5（可观察性）**：关键事件（恢复 N 条、截断尾部、淘汰、满盘降级、锁冲突、压缩）有结构化 logger 输出（V(1)/V(2) 分级与现有风格一致）并有对应计数器。

## Constraints
- **Technical**: Go 1.26；paho QoS 1 Token 语义不可变；`types.MQTTReloadState` 接口尽量保持兼容（新增能力通过具体类型/Options 传递，不破坏既有实现者）；改动需同时支持开放 MQTT 与 Bleemeo 共用的 `mqtt/client` 层。
- **Business**: 默认关闭，opt-in；不承诺 exactly-once；不改变现有默认部署的磁盘占用（关闭时零新增文件）。
- **Dependencies**: `github.com/gofrs/flock`（提升为直接依赖）；无新外部服务。

## Assumptions
- 下游（broker 与消费者）按 MQTT 规范容忍 QoS 1 重复；Glouton 不为重复重放做端到端去重。
- 状态目录所在文件系统支持 fsync、rename 与 flock（Glouton 支持的常规 Linux/Windows/macOS 文件系统均满足）。
- `retry=true` 集合即"需要崩溃恢复的消息"集合；如未来出现即发即弃但标 retry=true 的新消息类型，需要重新评审 FR-2 的范围。
- 容量默认 100 MiB / 7 天足以覆盖典型的 broker 中短期不可用窗口；用户可配置。

## Acceptance Criteria

### AC-1: 默认关闭与显式关闭时维持纯内存行为
- **Type**: `rule`
- **Given**: 配置中 `mqtt.spool.enable=false`、`bleemeo.mqtt.spool.enable=false`（均为默认）。
- **When**: agent 以任意工作目录启动、运行、reload、退出。
- **Then**: 状态目录下不创建 `mqtt-spool` 目录、WAL 或锁文件；待发消息只存在于现有 1000 条内存 FIFO；FIFO 满时 PutNoWait 失败丢弃并保持原日志；退出时残留消息仅记日志。
- **Pass Condition**: 全量运行（含现有测试）后文件系统无新增 spool 文件，且 `git diff` 确认关闭路径行为与当前代码等价（现有 mqtt client/reload 相关测试不改动语义即通过）。
- **Evidence**: 配置默认值检查、临时状态目录集成跑查（find 无 mqtt-spool）、`go test ./mqtt/... ./bleemeo/internal/mqtt/...`。

### AC-2: 落盘先于发布，确认前可恢复
- **Type**: `rule`
- **Given**: spool 启用。
- **When**: 一条 retry 消息被发送且进程在以下任一时刻被杀：DATA 写入前；DATA fsync 后、PUBACK 前；PUBACK 后、RETIRE fsync 前。
- **Then**: 第 1 种情况 broker 与 spool 都没有该消息（无凭空重放）；第 2、3 种情况下次启动重放该消息（可能造成 broker 收到两次）；RETIRE 已持久化的消息不重放。
- **Pass Condition**: 单元测试按三个时序点模拟崩溃（关闭/重开 spool 与可注入的发布/确认钩子），断言恢复集合与重放次数符合上表；代码审查确认 paho.Publish 调用在 DATA append+sync 成功之后。
- **Evidence**: spool 生命周期单测（含故障注入 FS）、client 发送路径测试、代码评审记录。

### AC-3: broker PUBACK 是唯一正常退休条件
- **Type**: `rule`
- **Given**: spool 启用，消息已发布。
- **When**: 未收到 PUBACK（broker 不可用、Token 出错、超时）或收到 PUBACK。
- **Then**: 未确认消息保持存活并按现有 ackOne 逻辑重发/重排队；确认后写 RETIRE 并回收内存；RETIRE 写失败时消息不退休（下次仍可确认/重试），并计数。
- **Pass Condition**: 测试模拟 Token 失败/超时/成功三种结果，断言 WAL 存活集合与内存回收行为。
- **Evidence**: client 层 ack 路径测试 + spool RETIRE 测试。

### AC-4: reload 与跨进程只有一个回放所有者
- **Type**: `rule`
- **Given**: spool 启用。
- **When**: 连续触发多次进程内 reload；以及第二个进程/第二个 ReloadState 尝试以同一 spool 路径打开。
- **Then**: 任一时刻只有一个 ackManager 在消费；恢复注入每个进程生命周期只发生一次，无重复注入；第二个锁持有者获取 flock 失败，诊断标记 locked 并降级内存，不发生双写。
- **Pass Condition**: synctest 单测覆盖"旧 run 未退出则新 run 不得注入/打开写句柄"（由现有 wg.Wait 屏障保证，测试中体现），flock 互斥有独立单测。
- **Evidence**: `go test -race` 下单测通过；代码评审显示 spool 依附 ReloadState 且仅在 run 屏障之后应用。

### AC-5: 容量与保留期限按确定规则淘汰且全部可观察
- **Type**: `rule`
- **Given**: spool 启用，配置 max_size_mb 与 max_age。
- **When**: 存活字节达到上限继续入队；记录年龄超过 max_age；单条记录大于上限。
- **Then**: 按最老优先淘汰直到放入；出队/恢复时超龄记录不发送而按年龄淘汰计数；超大单条被拒绝并计数；淘汰计数、被淘汰字节数、最老存活年龄可在诊断读取；任何被丢弃记录都对应一次显式淘汰/拒绝计数，不存在静默消失。
- **Pass Condition**: 单测构造确定性序列（小上限、可控时间戳），断言被淘汰的精确 seq 集合、计数与诊断输出；`max_age=0` 时不发生年龄淘汰。
- **Evidence**: spool 策略单测（表驱动、顺序断言）。

### AC-6: 截断/损坏尾部恢复有效前缀，坏记录分类计数
- **Type**: `rule`
- **Given**: 一个含若干有效帧的 WAL，其后分别拼接：截断的半帧；CRC 错误帧；CRC 正确但载荷不可解码的 DATA 帧；更高版本 magic 的文件。
- **When**: spool 打开。
- **Then**: 半帧/CRC 错误处恢复其前全部有效前缀、截断尾部并计 corrupt_tail；不可解码记录计 unrecoverable_records 且其后有效帧继续恢复；版本不识别时隔离原文件、新建 WAL 并计 unrecoverable。
- **Pass Condition**: 四种损坏各有单测，断言恢复记录集合、截断后文件字节、计数器与隔离文件存在性。
- **Evidence**: 恢复/扫描单测。

### AC-7: 满盘与编码失败分别暴露、降级不丢当前消息
- **Type**: `rule`
- **Given**: spool 启用。
- **When**: DATA append 返回 ENOSPC（注入故障）；JSON/zlib 编码失败（传入不可编码对象）；RETIRE 写入返回 ENOSPC。
- **Then**: ENOSPC 时发布仍走内存路径尝试（本次发送不被黑洞），disk_full 计数独立；编码失败时 PublishAsJSON/PublishBytes 返回错误（现有契约）且 encode_failed 计数独立；RETIRE ENOSPC 时消息保持存活；诊断 JSON 中三类计数字段各自独立存在。
- **Pass Condition**: 故障注入单测断言计数分类与"消息仍被尝试发布/错误仍被返回"。
- **Evidence**: client + spool 单测，诊断 JSON 字段断言。

### AC-8: 诊断归档呈现完整 spool 闭环状态
- **Type**: `rule`
- **Given**: 两条链路任一启用 spool 并产生过发送/恢复/淘汰事件。
- **When**: 生成诊断归档。
- **Then**: 归档中分别包含 open-source 与 bleemeo 的 spool 状态文件，字段至少有：enabled、draining、locked、path、file_bytes、live_records、inflight_records、oldest_record_age、recovered_records、evicted_size_records/bytes、evicted_age_records、disk_full_events、encode_failed_events、write_errors、corrupt_tail_records/bytes、unrecoverable_records、compactions、last_recovery_at；现有 mqtt-stats 与 mqtt-client.json 保留。
- **Pass Condition**: 单测构造归档（内存 ArchiveWriter）断言字段齐全且数值与事件一致。
- **Evidence**: DiagnosticArchive 单测。

### AC-9: 现有发送语义保持不变
- **Type**: `rubric`
- **Dimension**: 对既有发送链路行为的保持程度（批量取点、注册过滤、失败点年龄与清理、暂停发送/缓冲、背压、非重试控制消息、reload 待发点交接）。
- **Scale**: 1-5
- **Anchors**: 1 = 明显破坏既有语义且无测试兜底；3 = 语义保持但缺少针对性回归验证；5 = 全部既有语义保持，且每项均有原有测试或新增回归测试覆盖，`PendingMessagesCount` 在恢复注入后正确反映 backlog。
- **Pass Threshold**: >= 4
- **Evidence**: 现有 `mqtt`、`bleemeo/internal/mqtt`、agent 相关测试全绿 + 针对背压计数包含恢复消息的回归测试 + 代码评审。

### AC-10: 六类故障场景形成"确认前可恢复、明确淘汰才允许丢弃"的可观察闭环
- **Type**: `rubric`
- **Dimension**: broker 不可用、发布/确认间崩溃、正常 reload、干净退出再启动、队列损坏、容量耗尽六类场景下端到端行为的一致性与可观察性（日志、计数、诊断三者闭环）。
- **Scale**: 1-5
- **Anchors**: 1 = 场景未覆盖或存在静默丢失；3 = 场景可恢复但部分场景无日志/诊断线索；5 = 六类场景均有测试或明确推演证据，且每条消息去向（已确认退休/待发重放/明确淘汰计数/降级计数）均可由诊断数据解释。
- **Pass Threshold**: >= 4
- **Evidence**: 场景矩阵测试/推演表（链接到 review.md 的独立复核）。

### AC-11: 配置开关在 reload 时的行为确定且无数据真空
- **Type**: `rule`
- **Given**: 运行中切换 spool enable。
- **When**: 关→开 reload；开→关 reload；改容量参数 reload。
- **Then**: 关→开：打开恢复（无文件则为空）、内存 backlog 补登落盘，之后新消息持久化；开→关：进入 draining，不再落盘新消息，未确认存量继续由同一 ackManager 回放至排空后删除 WAL，期间诊断可见 draining；容量参数下次 run 生效（不需要重启进程）；任何转换过程中消息不丢失、不双发（重放注入不重复）。
- **Pass Condition**: synctest 单测覆盖三种转换，断言文件状态、计数、FIFO 内容与注入次数。
- **Evidence**: reload 配置应用单测。

### AC-12: 工程质量基线
- **Type**: `rule`
- **Given**: 全部实现完成。
- **When**: 执行构建、测试、竞态检测与静态检查。
- **Then**: `go build ./...`、`go test ./...`（至少受影响包）、`go test -race`（受影响包）、`go vet`/golangci-lint（可用时）通过；gofmt 干净；Windows 交叉编译通过；新依赖在 go.mod 中为直接依赖。
- **Pass Condition**: 命令输出记录在 tasks 完成证据中。
- **Evidence**: CI 等价命令输出。

## Open Questions
- 无（关键取舍已在"关键设计决策"中固化；如审批阶段对默认容量 100 MiB/168h、仅持久化 retry=true、关闭时 draining 策略有不同意见，在审批中提出并回退到 Specify 修订）。
