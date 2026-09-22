# MQTT 持久待发队列（Spool）- 实施计划

说明：每项任务的 Test Requirement（TR）类型仅为 `rule` 或 `rubric`；`rule` 由实现者给出可复现命令/证据并自验，`rubric` 记录分值、依据与证据。AC 映射见 spec.md。

## Issue I-1: 压缩重开后 fd 偏移为 0，后续写覆盖 WAL（R1 F-001）
- **Status**: `completed`
- **Priority**: high
- **Depends On**: None
- **Discovered By**: Review R1
- **Completion Evidence**:
  - `rule` TR-I-1.1: PASS。compactLocked 重开后 Seek(SeekEnd)、reopenLocked 同样 seek end、扫描截断后 Seek(validOffset)。新增 TestSpoolWritesAfterRecoveryCompaction（重启→追加 2→RETIRE→追加→重开，精确恢复 [2,3,4,5,6]）、TestSpoolWritesAfterInProcessCompaction（进程内压缩后追加，恢复 [5,6,7]），均 -race 通过。

## Issue I-2: Dispatch 标记与 FIFO 入队非原子，reload 可饿死记录（R1 F-002）
- **Status**: `completed`
- **Priority**: high
- **Depends On**: I-1
- **Discovered By**: Review R1
- **Completion Evidence**:
  - `rule` TR-I-2.1: PASS。Dispatch 改名 Lease（租约语义）+ Unlease 回滚游标；TestReloadStateLeaseUnlease：满 FIFO+canceled ctx 入队失败→unlease→undispatched=5，排空后重新 lease 恰好 5 条入队，无丢失无重复。
  - `rule` TR-I-2.2: PASS。fifo.Put 与 MQTTReloadState.AddPendingMessage 均返回 enqueued bool（接口同步更新）；spoolFeeder 入队失败归还本批剩余租约；publishWrapper 入队失败归还 seq；adoption PutNoWait 失败同样归还。

## Issue I-3: corrupt_tail_records 计数语义（R1 F-005，advisory）
- **Status**: `completed`
- **Priority**: low
- **Depends On**: None
- **Discovered By**: Review R1
- **Completion Evidence**:
  - `rule` TR-I-3.1: PASS。完整帧损坏计 1（CRC/魔异/未知类型/长度），半帧（header/body EOF）计 0，字节数照常；TestSpoolCorruptTailTruncated（0 条 +5 字节）、TestSpoolCorruptTailBadCRC（1 条）通过。

### R1 advisory 处置说明（不改码）
- F-003（recovered 与新消息可能乱序）：FR-4 仅要求注入按 seq 顺序，QoS1 不保证跨消息顺序，记录在案。
- F-004（time-drift /disconnect 为 retry=true 会持久化）：spec 假设已确认 retry=true 集合纳入持久化；陈旧 disconnect 重放是边缘产品语义，保持现状。
- F-005 其余（.lock 文件进程生命周期内保留）：flock 是跨进程互斥手段，drain 后保留锁可防止第二进程占用；WAL 按设计删除，诊断可见 locked 状态。


## Task 1: 配置结构、默认值与加载校验
- **Status**: `completed`
- **Priority**: high
- **Depends On**: None
- **Completion Evidence**:
  - `rule` TR-1.1: PASS。`go test ./config/...` 全绿；新增 TestMQTTSpoolDefault/Custom/InvalidValues（`config/spool_test.go`），默认 `{false,100,168h}`、`48h` duration 解析、负值告警回退均通过（`go test ./config/... -run 'MQTT|Spool' -v` 3/3 PASS）。
  - `rule` TR-1.2: PASS。simple.conf（无 spool 节）加载后两处取默认值，TestMQTTSpoolDefault 验证；旧 testdata 全部用例（内部 load 与公共 Load）无回归。
  - 变更：[config/types.go](file:///Users/vance/project/mineProject/swe/092112/project-01/config/types.go)（MQTTSpool 类型 + 两处字段）、[config/default.go](file:///Users/vance/project/mineProject/swe/092112/project-01/config/default.go)（DefaultMQTTSpool/常量/两处默认）、[config/config.go](file:///Users/vance/project/mineProject/swe/092112/project-01/config/config.go)（normalizeMQTTSpool）、testdata/mqtt-spool{,-invalid}.conf。
- **Description**:
  - 在 [config/types.go](file:///Users/vance/project/mineProject/swe/092112/project-01/config/types.go) 新增 `MQTTSpool` 结构（`Enable bool yaml:"enable"`、`MaxSizeMB int yaml:"max_size_mb"`、`MaxAge time.Duration yaml:"max_age"`），并为 `OpenSourceMQTT` 与 `BleemeoMQTT` 各增加 `Spool MQTTSpool yaml:"spool"` 字段。
  - 在 [config/default.go](file:///Users/vance/project/mineProject/swe/092112/project-01/config/default.go) 两处 MQTT 默认值中填入 `Enable:false, MaxSizeMB:100, MaxAge:168h`。
  - 在配置加载/校验路径（config.go 的 Load 后置处理或既有 mistake 检查）对 `max_size_mb<0`、`max_age<0` 给出与现有风格一致的 warning/error；零值沿用默认（max_size_mb=0 视为 100，max_age=0 表示不按年龄淘汰——注意区分：用户显式 0 与未设置无法区分，统一规定 0=不按年龄淘汰，因此默认值需在默认层显式给 168h，用户写 0 即关闭年龄淘汰；max_size_mb 不允许 0/负，回退默认并告警）。
  - 更新 [config/config_test.go](file:///Users/vance/project/mineProject/swe/092112/project-01/config/config_test.go) 中受影响的期望结构体（多处字面量），新增 testdata 或内联用例：默认值、自定义 spool（含 `"168h"` duration 字符串解析）、非法值告警。
- **Acceptance Criteria Addressed**: AC-1, AC-12（配置部分）, FR-1
- **Test Requirements**:
  - `rule` TR-1.1: `go test ./config/...` 通过；默认配置断言两处 Spool 均为 `{false,100,168h}`；自定义 YAML 可被解析；非法 max_size_mb 产生可观察的 warning/error 且不 panic。证据：测试输出。
  - `rule` TR-1.2: 不含 spool 节的旧配置文件加载后取默认值（向后兼容）。证据：新增/既有 testdata 用例输出。

## Task 2: 磁盘 Spool（WAL）核心实现与单测
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 1
- **Completion Evidence**:
  - `rule` TR-2.1: PASS。新增 [spool.go](file:///Users/vance/project/mineProject/swe/092112/project-01/mqtt/client/spool.go)（分帧/CRC/Append/Retire/Dispatch/evict/compact/Close）+ 跨平台 errno（spool_errno_{unix,windows}.go）；`go test -race -count=1 ./mqtt/client/` 全绿（17 个 Spool 用例 + 既有用例）。
  - `rule` TR-2.2: PASS。TestSpoolCorruptTailTruncated（半帧截断 5 字节）、TestSpoolCorruptTailBadCRC（CRC 错恢复前 1 帧）、TestSpoolUndecodableRecordSkipped（CRC 合法 body 非法→unrecoverable=1 且后续帧继续恢复）、TestSpoolUnknownVersionQuarantined（隔离文件 `<name>.wal.corrupt-<ts>`、新 seq 从 1 开始）。
  - `rule` TR-2.3: PASS。TestSpoolSizeEviction（100 字节上限→精确淘汰 seq1 35B、计数 1，重开不复活）、TestSpoolOversizedRecord、TestSpoolMaxAge（2h 超龄恢复/出队计数，0 关闭）、TestSpoolMaxAgeDisabled。
  - `rule` TR-2.4: PASS。TestSpoolFilePermissions 断言目录 0700、WAL/lock 0600。
  - 其他：TestSpoolFlockExclusivity（第二持有者 errSpoolLocked + locked 占位拒绝 reopen）、TestSpoolDiskFullOnAppend（ENOSPC 分类 ErrSpoolDiskFull）、TestSpoolCompaction（RETIRE 后压缩、顺序/seq 不变）、TestSpoolConcurrent（20×20 -race）。
- **Description**:
  - 在 `mqtt/client` 下新增 spool 实现（建议新文件 `spool.go` + `spool_test.go`），包含：
    - 二进制分帧：`magic(2)|version(1)|type(1: DATA/RETIRE)|length(4)|crc32(4)|body`；DATA body = `seq u64 | enqueuedUnixNano u64 | topicLen u16+topic | payloadLen u32+payload`；RETIRE body = `seq u64`。
    - `Open(ctx, opts)`：`MkdirAll(stateDir/mqtt-spool, 0o700)`；`gofrs/flock` 对 `<name>.lock` 取排他锁（失败返回明确的 sentinel error，由上层降级）；以 0o600 打开/创建 `<name>.wal`；顺序扫描恢复（见下）；恢复后做一次压缩；按 seq 顺序把存活记录交给调用方注入。
    - 扫描：首个魔数/版本不符（整文件版本不识别）→ quarantine（rename 为 `<name>.wal.corrupt-<unixnano>`）并新建空 WAL，计 unrecoverable；半帧（EOF 提前）或 CRC 错 → 停止扫描，ftruncate/重写到有效偏移，计 corrupt_tail 记录数与字节数；CRC 正确但 DATA body 解码失败 → 计 unrecoverable_records 并继续其后扫描。
    - `Append(topic, payload, enqueuedAt)`：先按容量规则淘汰（存活字节口径，最老优先；年龄在恢复与出队层处理，入队时也可即时拒绝超龄新记录——新记录不可能超龄，故只做大小淘汰），再写 DATA 帧并 fsync；单条大于上限返回 `ErrSpoolRecordTooLarge`；ENOSPC 返回可 `errors.Is(..., errSpoolDiskFull)` 判定的错误；返回单调 seq。写锁互斥。
    - `Retire(seq)`：追加 RETIRE 帧并 fsync；更新内存存活/inflight 视图；写失败保留存活（不清除索引）并返回错误。
    - `MarkInflight/Evict/IsEvictedOrExpired` 等查询接口供 client 层做确定性淘汰决策；年龄判断以 enqueuedAt 与配置 maxAge 为准（maxAge=0 关闭）。
    - `Compact()`：死记录（RETIRE 抵消、淘汰）比例或文件字节超阈值（确定性阈值：文件字节 ≥ 2×存活字节，或存活字节为 0 且文件非空）时，在写锁内写 `<name>.wal.tmp`（0o600）→ fsync → rename → fsync 目录；记录 compactions 计数。
    - `Snapshot() SpoolStats`：enabled/draining/locked/path、fileBytes、live/inflight 计数与字节、oldestEnqueuedAt、recovered、evictedSize(records/bytes)、evictedAge、diskFull、encodeFailed（编码失败在 client 层计数，spool 仅保留 body 编解码失败的 unrecoverable）、writeErrors、corruptTail(records/bytes)、unrecoverable、compactions、lastRecoveryAt。
    - `Close()`：幂等；flush、关闭文件、释放 flock；draining 排空后删除 WAL 与 lock 文件的策略由上层调用 `RemoveIfEmpty()` 触发。
  - 将 `github.com/gofrs/flock` 提升为 go.mod 直接依赖（`go mod tidy`，离线环境下模块已在 go.sum/模块缓存中）。
  - 纯文件/tmpdir 单测，不依赖网络与 paho：
    - append/retire 基本往返；单调 seq；fsync 后重开可见。
    - 崩溃序列：DATA 后关闭→重开重放；RETIRE 后关闭→重开为空。
    - 截断半帧、中间 CRC 错、body 不可解码、版本不识别 quarantine 四用例（AC-6）。
    - 容量淘汰：小字节上限下精确断言被淘汰 seq 集合与计数；超大单条 sentinel（AC-5）。
    - maxAge 边界（0 关闭、正超期）通过可注入时钟/直接构造 enqueuedAt 验证。
    - ENOSPC/IO 错误注入（用 `writeErrorInjector` 包装文件或接口注入），分类断言（AC-7 的 spool 部分）。
    - compact 阈值与重写后内容、顺序、seq 不变；并发 Append/Retire/Compact 不损坏（`-race`）。
    - flock：第二次 Open 同路径返回锁错误（AC-4 跨进程部分）。
- **Acceptance Criteria Addressed**: AC-2, AC-3, AC-4（flock）, AC-5, AC-6, AC-7, FR-2~FR-8, NFR-2/3/4
- **Test Requirements**:
  - `rule` TR-2.1: `go test -race -count=1 ./mqtt/client/ -run 'Spool|WAL'` 全绿，覆盖上述全部用例。证据：命令输出与用例列表。
  - `rule` TR-2.2: 损坏恢复四用例精确断言"恢复集合 + 截断字节 + 计数器 + quarantine 文件"。证据：测试断言代码与输出。
  - `rule` TR-2.3: 容量/年龄用例在确定性输入下断言精确 seq 集合，无随机性。证据：表驱动测试输出。
  - `rule` TR-2.4: 生成的目录为 0700、WAL/tmp/lock 为 0600（`os.Stat` Mode().Perm()）。证据：测试输出。

## Task 3: client 层集成（发送/确认顺序、淘汰决策、reload 生命周期、诊断）
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 2
- **Completion Evidence**:
  - `rule` TR-3.1: PASS。`go test -race -count=1 ./mqtt/client/` 全绿；既有 fifo/state/encoding/OnConnectionLost 测试语义未改（仅新增 fifo.Drain）。
  - `rule` TR-3.2: PASS。TestPersistBeforeEnqueue：blockingWAL 证明 DATA 写完成前 FIFO 计数为 0（持久化先于入队/发布）；faultWAL 注入 ENOSPC 时消息仍入 FIFO（seq=0、内存降级）且 DiskFullEvents 计数。
  - `rule` TR-3.3: PASS。TestReloadStateSpoolAdoption（关→开 backlog 补登 5 条全部带 seq，再次 Apply 不重复）、TestReloadStateSpoolDisableDrains（draining 拒新写、排空删 WAL、再开重建）、TestClientRunReloadSpoolSingleOwner（两次完整 Run 后 RecoveredRecords=3，FIFO+undispatched=3 无重复无丢失）。
  - `rule` TR-3.4: PASS。TestSpoolDiagnosticArchive 断言 23 个字段齐全（含 enabled/draining/locked/available/三类故障计数/encode_failed_events/最老年龄），NaN 编码失败返回原错误且计数 1。
  - `rubric` TR-3.5: 4/5。关闭路径与开启路径共享同一 FIFO 与 ackOne 代码，spool 仅在 publishWrapper/ackOne 两个边界接入；关闭等价性由 TestOpenMQTTSpoolWiring（零文件）与既有测试全绿共同保证；扣 1 分：非真实 broker 的端到端 PUBACK 往返由代码评审与组件测试替代。
  - 额外加固：RETIRE/年龄淘汰写失败不回收 payload（sendRetryEviction 重排队，TestSpoolAgeEvictionFailureRetries 覆盖）；ReloadState.Close 覆盖 client==nil 场景；spool 关闭纳入 Bleemeo 关闭链。
- **Description**:
  - 扩展 `client.Options`：增加 `Spool SpoolConfig`（Enabled、MaxSizeBytes、MaxAge、Directory、Name——Name 由 ID 派生：`open-source`/`bleemeo`）；`client.NewReloadState()` 保持可无参用于测试，新增 `NewReloadStateWithSpool(...)` 或在 `ReloadState` 上提供幂等 `ApplySpool(cfg) (*spool, error)`（由 `client.New` 在每次 run 调用，同一 ReloadState 仅首次真正打开；之后按 AC-11 切换 enabled/draining）。
  - 恢复注入只发生在 spool 首次打开的同一进程生命周期：恢复出的记录构造为 `types.Message{Retry:true, Topic, Payload, SpoolSeq, EnqueuedAt}`，按 seq 注入现有 FIFO（Put，Token=nil）；同时把 FIFO 中尚无 seq 的 retry 消息补登 Append（关→开场景），补登失败按满盘/写错误规则处理。
  - 修改 `publishWrapper`/`publish` 顺序（AC-2）：大小检查后，若 spool 可写且 retry=true，先 `Append`（成功设置 msg.SpoolSeq）；ENOSPC→计数 disk_full 并继续发布（降级内存）；其他写错误→write_errors 同样降级；然后 `mqtt.Publish` 设置 Token；最后 `AddPendingMessage`。retry=false 路径完全不变。
  - 修改 `ackOne`：出队先查 spool 状态——seq 已被大小淘汰或超龄 → 不发送，计数 evicted_age/evicted_size，回收 buffer 返回；Token=nil/出错的重发逻辑保持（重发不新增 DATA 帧）；PUBACK 成功路径在回收 buffer 前调用 `Retire(seq)`，RETIRE 失败则消息重新入队等待下轮再确认退休（不回收、不丢）。
  - 编码失败计数：`PublishAsJSON/PublishBytes` encode 出错时累加 `encode_failed` 再返回原错误（契约不变）。
  - 在 `DiagnosticArchive` 新增 `<fileID>-mqtt-spool.json`（字段按 AC-8）；现有 stats 与 client.json 不动；`PendingMessagesCount()` 语义不变（恢复注入后天然包含 backlog，AC-9 背压自动保持）。
  - 单测（不依赖真实 broker，使用 nil paho client + 可注入 Token 假象/直接操作 ReloadState 与 synctest）：
    - 落盘先于发布：用故障/计数钩子断言 Publish 发生在 Append 成功之后；Append 失败时仍尝试发布且 disk_full 计数。
    - 出队淘汰/超龄跳过；RETIRE 失败重入队不丢；正常确认退休后 spool 快照为空。
    - ApplySpool 三次调用（enabled→enabled、关→开、开→关）幂等且不产生第二次恢复注入；draining 时新消息不落盘、存量继续排空，排空后文件删除（AC-11）。
    - 诊断 JSON 字段与计数快照正确（AC-8）。
    - 关闭路径：`ReloadState.Close()` 后 spool 已 Close/释放锁；再开一个 ReloadState 可成功恢复（模拟干净退出后启动）。
- **Acceptance Criteria Addressed**: AC-2, AC-3, AC-4（进程内单所有者）, AC-7, AC-8, AC-11, FR-2/3/4/5/8/9/12, NFR-1/5
- **Test Requirements**:
  - `rule` TR-3.1: `go test -race -count=1 ./mqtt/client/` 全绿（含既有 fifo/state/encoding 测试不改语义）。证据：命令输出。
  - `rule` TR-3.2: 存在一个测试断言"Append 成功之前不存在 paho.Publish 调用"（用计数钩子），以及 ENOSPC 时发布仍发生且计数独立。证据：测试代码与输出。
  - `rule` TR-3.3: AC-11 的三种配置转换各有 synctest 用例，断言：恢复注入恰好一次、draining 期间无新 DATA 帧、排空后 WAL 文件消失、转换中无消息丢失（FIFO+spool 并集不变）。证据：测试输出。
  - `rule` TR-3.4: 诊断测试用内存 ArchiveWriter 断言 AC-8 全部字段存在且数值与注入事件一致。证据：测试输出。
  - `rubric` TR-3.5: 与既有内存路径的行为一致性；scale 1-5；anchors 1=关闭路径出现行为分叉；3=行为一致但靠人工推演；5=关闭/开启双路径均有自动化对照测试且共享同一 FIFO/ack 代码；threshold >=4；证据：对照测试与代码评审。

## Task 4: 两条链路与 agent 的装配接线
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 3
- **Completion Evidence**:
  - `rule` TR-4.1: PASS。`go build ./...`（临时补 gitignored 的 api/assets 占位后）全绿；`GOOS=windows GOARCH=amd64 go build ./...` 全绿；errno/flock 均跨平台。
  - `rule` TR-4.2: PASS。`go test ./mqtt/... ./bleemeo/internal/mqtt/... ./agent/... ./config/...` 全绿；bleemeo TestFailedPointsCache/TestMQTTPointOrder、agent/config 既有用例零适配失败（配置结构体新增字段未破坏比较，因为默认在公共 Load 规范化）。
  - `rule` TR-4.3: PASS。TestOpenMQTTSpoolWiring：enable 时构造连接器即生成 `<StateDirectory>/mqtt-spool/open-source.wal` 与 `.lock`；disable（默认）时无 mqtt-spool 目录。接线位置：[mqtt/mqtt.go](file:///Users/vance/project/mineProject/swe/092112/project-01/mqtt/mqtt.go)（Options.StateDirectory + Spool 透传）、[agent/agent.go](file:///Users/vance/project/mineProject/swe/092112/project-01/agent/agent.go#L1433-L1440)、[bleemeo/internal/mqtt/mqtt.go](file:///Users/vance/project/mineProject/swe/092112/project-01/bleemeo/internal/mqtt/mqtt.go#L204-L215)（bleemeo 名，Config.Agent.StateDirectory + Config.Bleemeo.MQTT.Spool）。Bleemeo Close 链补 clientState.Close。
- **Description**:
  - 开放 MQTT：[mqtt/mqtt.go](file:///Users/vance/project/mineProject/swe/092112/project-01/mqtt/mqtt.go) `Options` 增加 `Spool config.MQTTSpool` 与 `StateDirectory string`；`mqtt.New` 构造 `client.Options` 时传入 SpoolConfig（Name=`open-source`，目录=StateDirectory，MaxAge/MaxSizeBytes 换算）。[agent/agent.go](file:///Users/vance/project/mineProject/swe/092112/project-01/agent/agent.go#L1433-L1439) 装配处传入 `a.config.MQTT.Spool` 与 `a.config.Agent.StateDirectory`。开放 MQTT 的 10s tick、1000/批逻辑零改动。
  - Bleemeo：[bleemeo/internal/mqtt/state.go](file:///Users/vance/project/mineProject/swe/092112/project-01/bleemeo/internal/mqtt/state.go) `ReloadStateOptions` 增加 spool 配置与状态目录；`NewReloadState` 创建 `clientState` 时传入（Name=`bleemeo`）；[bleemeo/internal/mqtt/mqtt.go](file:///Users/vance/project/mineProject/swe/092112/project-01/bleemeo/internal/mqtt/mqtt.go#L194-L211) 首次创建处从 `opts.Config.Agent.StateDirectory` 与 `opts.Config.Bleemeo.MQTT.Spool` 取值；每次 `client.New` 经 client.Options 传递当期配置以支持 reload 切换。
  - 保持 bleemeo `pendingPoints` reload 交接（Pop/SetPendingPoints）、failedPoints 全部逻辑不变。
  - agent 层 [agent/reloader.go](file:///Users/vance/project/mineProject/swe/092112/project-01/agent/reloader.go#L240-L243) 的进程级 `client.NewReloadState()` 保持无参（配置在 client.New 时 Apply），或按 Task 3 定型的最小侵入方式调整；确保进程退出 Close 路径释放两个 spool。
  - 核对 Windows 路径拼接（filepath.Join）与文件锁行为，无 unix 专有调用。
- **Acceptance Criteria Addressed**: AC-1, AC-9, AC-11, FR-10/11/12, NFR-2/3
- **Test Requirements**:
  - `rule` TR-4.1: `go build ./...` 与 `GOOS=windows GOARCH=amd64 go build ./...` 均通过。证据：命令输出。
  - `rule` TR-4.2: 既有 `mqtt`、`agent`、`bleemeo` 包测试不新增失败；如需调整构造字面量（mock/options），仅做适配性修改。证据：`go test ./mqtt/... ./agent/... ./bleemeo/...` 输出。
  - `rule` TR-4.3: 端到端轻量装配验证（可在 agent 或 mqtt 包用临时目录 + nil broker 跑一次 Run 生命周期）：enable=true 生成 `mqtt-spool/open-source.wal`（Bleemeo 侧为 `bleemeo.wal`）；enable=false 不生成任何文件。证据：测试输出与临时目录文件列表。

## Task 5: 既有发送语义回归与六场景可观察闭环验证
- **Status**: `completed`
- **Priority**: medium
- **Depends On**: Task 4
- **Completion Evidence**:
  - `rule` TR-5.1: PASS。TestCanSendLogsBackPressureWithBacklog（51 条 pending → canSendLogs=false，排空 + 近期 ack → true；spool 恢复消息经 feeder 进入同一 FIFO，计数天然包含）、TestCanSendLogsBackPressureWithStaleAck（旧 ack 独立背压）。
  - `rule` TR-5.2: PASS（自动化 + 推演矩阵，每条消息去向均可由计数字段解释）：
    1. **broker 不可用**：TestOpenMQTTBatchingPreserved（2001 点无 broker → 3 条批次全部进入 FIFO，无丢失）、TestPersistBeforeEnqueue（落盘后入队，fileBytes/live 增长）；恢复连接后由既有 ackOne 重发逻辑排空。
    2. **发布/确认间崩溃**：TestSpoolRecoveryReplayAndRetire（3 条记录模拟进程退出，重开 seq 顺序重放，Token=nil 触发 QoS 1 重发——broker 可能收到重复；RETIRE 已落盘的 seq1 不重放）。
    3. **正常 reload**：TestClientRunReloadSpoolSingleOwner（两次真实 Run+取消，RecoveredRecords 恰为 3 不翻倍；FIFO 与 undispatched 之和恒为 3；run 屏障 wg.Wait 保证单消费者）。
    4. **干净退出再启动**：同 2，Close fsync 后释放锁；第三进程重放 2 条未确认记录，已确认 1 条不重放。
    5. **队列损坏**：四个损坏用例（截断/CRC/坏 body/版本隔离），corrupt_tail_*、unrecoverable_records、quarantined_files 计数与 V(1) 日志可观察。
    6. **容量耗尽**：TestSpoolSizeEviction/Oversized/MaxAge——evicted_size_records/bytes、evicted_age_records 增长；诊断 live/inflight/oldest_age 可解释每条去向（live=待发、inflight=在飞、淘汰计数=显式丢弃、disk_full=降级）。
  - `rubric` TR-5.3: 4/5。批量（TestOpenMQTTBatchingPreserved）、注册过滤/失败点（TestFailedPointsCache、TestMQTTPointOrder 原有用例未改且全绿）、暂停/缓冲与背压（新增 2 用例）、恢复计数入背压（TR-5.1）均有覆盖；扣 1 分：SuspendSending/SuspendBuffering 依赖完整 Bleemeo 运行时，由代码零改动评审 + 原测试套件间接保证，未做专门 spool 交互用例。
- **Description**:
  - 为 AC-9 增补/核对回归：开放 MQTT `pointsBatchSize` 分批行为（构造 >1000 点断言发布次数与 topic 不变）；Bleemeo `preparePoints` 注册过滤、未注册进 failedPoints、`failedPointsMaxAge` 1h 剔除、cleanup、`SuspendSending`/`SuspendBuffering`、ack 背压（`dataAckBackPressureDelay`）、`canSendLogs` 的 `PendingMessagesCount()>50` 阈值在**恢复注入 backlog** 时同样生效。
  - 为 AC-10 建立六场景验证矩阵（测试或可复演的集成脚本式测试，优先用 synctest + tmpdir + nil/假 broker 钩子）：
    1. broker 不可用：消息持续落盘，计数与文件增长可观察，连接恢复后排空；
    2. 发布后确认前"崩溃"：关闭 client/ReloadState（模拟进程退出）后重新 Open，消息重放且 broker 侧可能收到两次（断言发送次数=2 的场景存在）；
    3. 正常 reload：run1→cancel→run2，连接/队列保留，无重复注入、无消息丢失；
    4. 干净退出再启动：RETIRE 已落盘者不重放，未确认者重放；
    5. 队列损坏：预置坏尾 WAL 启动，有效前缀恢复、计数与日志可观察；
    6. 容量耗尽：小上限下持续入队，最老淘汰计数增长，诊断可解释每条消息去向。
  - 将场景矩阵作为 review 证据链接（测试名/输出）。
- **Acceptance Criteria Addressed**: AC-9, AC-10
- **Test Requirements**:
  - `rule` TR-5.1: 背压回归测试：恢复注入使 PendingMessagesCount 达 51 时 `canSendLogs` 返回 false/backpressure，排空后恢复。证据：测试输出。
  - `rule` TR-5.2: 六场景矩阵每项至少有一个自动化用例或（确实无法自动化者）带完整复现步骤的推演记录，并映射到具体计数字段。证据：测试名列表 + 输出；推演记录写入完成证据。
  - `rubric` TR-5.3: 既有语义保持评分；scale 1-5；anchors 同 AC-9；threshold >=4；证据：全量相关测试 + 评审。

## Task 6: 全量工程验证与收尾
- **Status**: `completed`
- **Priority**: medium
- **Depends On**: Task 5
- **Completion Evidence**:
  - `rule` TR-6.1: PASS（除环境项外）。
    - `gofmt -l mqtt/ config/ types/ agent/ bleemeo/`：无输出（干净）。
    - `go vet`：新增/修改文件零告警；仅存 `agent/reloader.go:328` 的**预存**告警（该文件本次未修改，已通过 git diff 确认）。
    - `go test ./...`（全仓库，exit 0，无 FAIL）；受影响包另跑 `go test -race -count=1` 全绿（mqtt、mqtt/client、bleemeo/internal/mqtt、config）。
    - `GOOS=windows GOARCH=amd64 go build ./...` 通过。
    - golangci-lint：环境未安装（`golangci-lint not found`），以 gofmt + go vet 替代，记入环境限制。
    - 预存环境问题：`api/assets`（gitignored 发布生成目录）缺失导致裸 `go build ./...` 失败，与本次改动无关（stash 验证）；本地验证时临时创建占位、验证后已删除。
  - `rule` TR-6.2: AC 覆盖矩阵：AC-1→TR-1.1/1.2/4.3；AC-2→TR-3.2/2.1、场景 2；AC-3→TR-2.1/3.2；AC-4→TR-2.1(flock)/3.3、场景 3；AC-5→TR-2.3、场景 6；AC-6→TR-2.2、场景 5；AC-7→TR-3.2/2.1、TestSpoolAgeEvictionFailureRetries；AC-8→TR-3.4；AC-9→TR-5.1/5.3、TestOpenMQTTBatchingPreserved；AC-10→TR-5.2 六场景；AC-11→TR-3.3；AC-12→TR-6.1。无缺口。
- **Description**:
  - `gofmt`/`goimports` 干净；`go vet ./...`；golangci-lint（环境可用则按 .golangci.yml 运行，不可用则记录）。
  - `go test -race -count=1 ./mqtt/... ./bleemeo/internal/mqtt/... ./config/... ./agent/...` 与全量 `go test ./...`（若环境允许）。
  - go.mod 直接依赖整理复核；无多余文件（spool 关闭零新增运行时文件）。
  - 自查 spec 每条 AC 的证据齐备，更新 tasks 完成证据，移交独立 Review。
- **Acceptance Criteria Addressed**: AC-12
- **Test Requirements**:
  - `rule` TR-6.1: 上述命令全部通过或对不可用项有明确环境说明；输出粘贴到 Completion Evidence。证据：命令输出。
  - `rule` TR-6.2: AC→TR→证据可追溯矩阵无缺口（每个 AC 至少被一个通过的 rule/rubric 覆盖）。证据：本文件完成证据汇总。
