# MQTT 持久待发队列（Spool）- Independent Review

- [ ] CP-R1: 默认关闭与显式关闭时纯内存行为等价（不创建任何 spool 文件，FIFO/退出语义不变）
  - **Type**: `rule`
  - **Covers**: AC-1, TR-1.2, TR-4.3
  - **Evidence**: Pending

- [ ] CP-R2: DATA 落盘（append+fsync）严格先于 paho.Publish；三个崩溃窗口的恢复集合正确（DATA 前=无，PUBACK 前/RETIRE 前=重放且允许重复，RETIRE 后=不重放）
  - **Type**: `rule`
  - **Covers**: AC-2, TR-3.2, 场景 2/4
  - **Evidence**: Pending

- [ ] CP-R3: broker PUBACK 是唯一正常退休条件；RETIRE 帧持久化失败时消息不退休、不回收、重排队；重发不新增 DATA
  - **Type**: `rule`
  - **Covers**: AC-3, TR-2.1, TR-3.2
  - **Evidence**: Pending

- [ ] CP-R4: reload 与跨进程均只有一个回放所有者（wg.Wait 屏障 + spool 依附 ReloadState + Dispatch 游标单注入 + flock 拒绝第二持有者/locked 不 reopen）
  - **Type**: `rule`
  - **Covers**: AC-4, AC-11, TR-3.3, 场景 3
  - **Evidence**: Pending

- [ ] CP-R5: 大小上限最老优先淘汰、max_age 到期淘汰、超大单条拒绝，均为确定性规则且分类计数；无静默丢弃
  - **Type**: `rule`
  - **Covers**: AC-5, TR-2.3, 场景 6
  - **Evidence**: Pending

- [ ] CP-R6: 截断/CRC 坏尾恢复有效前缀并截断；坏 body 单条跳过计数；版本不识别隔离；计数器区分
  - **Type**: `rule`
  - **Covers**: AC-6, TR-2.2, 场景 5
  - **Evidence**: Pending

- [ ] CP-R7: 满盘（ENOSPC）、编码失败、不可恢复记录三类独立计数；满盘降级为内存发布而非黑洞
  - **Type**: `rule`
  - **Covers**: AC-7, TR-3.2
  - **Evidence**: Pending

- [ ] CP-R8: 两条链路诊断归档新增 `<id>-mqtt-spool.json`，AC-8 全部字段存在且数值可信；既有 stats/client.json 保留
  - **Type**: `rule`
  - **Covers**: AC-8, TR-3.4
  - **Evidence**: Pending

- [ ] CP-U1: 既有发送语义保持（10s/1000 点分批、注册过滤、失败点 1h/100k、Suspend、ack/日志背压、retry=false 控制消息不落盘、reload 待发点交接）
  - **Type**: `rubric`
  - **Covers**: AC-9, TR-5.1, TR-5.3
  - **Scale**: 1-5
  - **Anchors**: 1 = 发现既有语义被破坏或无回归证据；3 = 语义保持但仅有人工推演；5 = 每项语义有代码+测试证据，PendingMessagesCount 背压在恢复 backlog 下验证
  - **Pass Threshold**: >= 4
  - **Evidence**: Pending

- [ ] CP-U2: 六类故障场景形成"确认前可恢复、明确淘汰才允许丢弃"的可观察闭环
  - **Type**: `rubric`
  - **Covers**: AC-10, TR-5.2
  - **Scale**: 1-5
  - **Anchors**: 1 = 存在静默丢失或场景不可观察；3 = 可恢复但部分场景缺日志/计数；5 = 六场景均可测试或推演，每条消息去向能由诊断字段解释
  - **Pass Threshold**: >= 4
  - **Evidence**: Pending

- [ ] CP-R9: 配置开关 reload 语义确定（关→开补登、开→关 draining、容量参数随 run 生效），转换中不丢不双发；默认值与非法值处理正确
  - **Type**: `rule`
  - **Covers**: AC-11, AC-1(配置), TR-1.1, TR-3.3
  - **Evidence**: Pending

- [ ] CP-R10: 工程基线（构建、全量/竞态测试、vet、gofmt、Windows 交叉编译、直接依赖、关闭时零新增文件）
  - **Type**: `rule`
  - **Covers**: AC-12, TR-4.1, TR-6.1
  - **Evidence**: Pending

## Review History

### Review R1
- **Result**: `fail`
- **Evidence**:
  - fail 检查点：CP-R2、CP-R3（F-001：压缩重开 fd 偏移 0，后续写覆盖 WAL，critical）、CP-R9（F-002：lease 与入队非原子，reload 可饿死记录，major）。
  - pass：CP-R1、CP-R4、CP-R5、CP-R6、CP-R7、CP-R8、CP-R10；CP-U1 4/5、CP-U2 2/5。
  - advisory：F-003（恢复/新消息顺序）、F-004（time-drift disconnect retry=true 持久化）、F-005（corrupt_tail_records 语义/lock 文件）。
  - 修复：I-1（Seek end ×3）、I-2（Lease/Unlease + AddPendingMessage 返回 bool + 回滚）、I-3（计数语义），均已完成并新增回归测试（TestSpoolWritesAfterRecoveryCompaction/InProcessCompaction、TestReloadStateLeaseUnlease），全仓库 `go test ./...` 无 FAIL、受影响包 -race 全绿、Windows 交叉编译通过。

### Review R2
- **Result**: `pass`
- **Evidence**:
  - F-001 已修复并经独立探针验证（rename 后 Seek(end) ×3 处，独立帧解析器校验多进程 Append/Retire 序列）；F-002 已修复（Lease/Unlease 租约语义 + 入队 bool + 三处回滚），无重复/无饿死；F-005 计数语义已修正。
  - 12 个检查点全部 pass：CP-R1..CP-R10 全 pass；CP-U1 4/5、CP-U2 4/5（扣分均为无真实 broker 端到端的既有边界，非缺陷）。
  - 基线：`go test -race`（mqtt/bleemeo-mqtt/config）全 ok；`go test ./...` 无 FAIL；`GOOS=windows go build ./...` 通过；gofmt/vet 干净。
  - R2 提出 1 个 advisory（压缩 rename 后 reopen 失败会写 unlinked inode）→ 已主动加固：failoverAfterLostWALLocked 丢弃失效 fd 并置 unavailable，enable 时按 stat 重开；新增 TestSpoolCompactionReopenFailureFailsOver 覆盖（failover 期间写入被拒，恢复后压缩前缀与新追加记录精确恢复）。Nit（FIFO 满载 drop 无独立计数器、陈旧注释）：注释已修正；计数器属预存语义，由 inflight_records 与日志可观察，不改。
  - 最终复跑：`go test ./...` exit 0；受影响包 -race 全绿；Windows 交叉编译通过；gofmt clean。
