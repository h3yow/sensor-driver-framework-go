# 设计说明：生命周期、健康度、分帧容错与本地验证

本文档约定框架的核心语义与规则。所有行为均有自动化测试覆盖，且完全本地运行。

## 1. 设备生命周期（`session` 包）

### 1.1 状态机

```
Idle ──Connect──► Connecting ──拨号成功──► Connected
                      │                       │
                      └──拨号失败──► Disconnected ◄──读错误/写错误/对端关闭/空闲隔离
                                           │
                                      Reconnect（唯一允许的入口）
                                           ▼
                                      Connecting ──► Connected
任意状态 ──Close──► Closed（终态）
```

- **Idle**：初始状态，未建立连接。
- **Connected**：读循环存活，唯一允许读写的状态。
- **Disconnected**：一次连接因故障终止；这是**唯一**允许 `Reconnect` 的状态。
- **Closed**：终态，不可恢复。

### 1.2 拒绝语义（可区分的失败原因）

| 操作                | 条件                          | 返回错误（哨兵）        |
|---------------------|-------------------------------|-------------------------|
| `Connect`           | 非 Idle 启动                  | `ErrAlreadyStarted`     |
| `Reconnect`         | 不在 Disconnected             | `ErrReconnectState` |
| `Write`             | 未连接                        | `ErrNotConnected`       |
| 任意操作            | Closed（含重复关闭/启动）      | `ErrClosed`（可重入关闭）|
| 建立连接失败         | 拨号错误                      | `ErrDialFailed`（join） |
| 已连接写出错         | 链路不可信                    | `ErrWriteFailed`，同时转 Disconnected |
调用方使用 `errors.Is` 即可精确区分失败原因。重复 `Close` 返回 `ErrClosed`
但不会 panic；关闭会等待读循环退出，保证同一时刻至多一个读 goroutine。

### 1.3 不崩溃、不卡死的保证

- 读循环内 `panic` 会被 `recover` 并转化为 Disconnected，绝不传播到进程。
- 每次（重）连接创建**全新的 `frame.Decoder`**，半包字节等旧解析状态不跨连接携带。
- `Reconnect` 会先等旧读循环完全退出再拨号，避免双读 goroutine 与状态竞争。
- 空闲超时（`ReadIdle`）本身不断开连接，仅计数并上报事件；由上层 `driver`
  按 `MaxStalls` 决定是否隔离，避免一次瞬时抖动造成无谓重连。

## 2. 分帧与容错（`frame` 包）

### 2.1 帧格式（小端序）

```
+--------+------+--------+----------+------------+-----------------+-------+
| magic  | type | seq    | ts       | payloadLen | payload         | crc16 |
| 2 字节 | 1 B | 2 byte | 8 byte   | 2 byte     | payloadLen 字节 | 2 B   |
+--------+------+--------+----------+------------+-----------------+-------+
```

- `magic = 0xA55A`（线上 `5A A5`）；`type ∈ {0x01 读数, 0x02 应答, 0x03 事件}`。
- `crc16` 为 CRC-16/MODBUS（多项式 0xA001，初值 0xFFFF），
  覆盖 `type..payload`，**不含** magic 与 crc 自身。
- `payloadLen` 上限 `MaxPayloadLen=512`。

### 2.2 容错规则

| 情形        | 处理                                                                 | 是否计坏帧 |
|-------------|----------------------------------------------------------------------|-----------|
| 半包        | 帧头/帧体未收全：保留已收字节等待后续 `Feed`，静默等待                 | 否        |
| 粘包        | 一次 `Feed` 中连续解析多帧                                            | —         |
| 坏 magic    | 逐字节扫描，重新同步到下一个 magic                                    | 是        |
| 长度越界    | 当前帧不可信；一次性跳转到下一 magic 重新同步（不逐字节报噪音）        | 是        |
| CRC 失败    | 丢弃当前帧，从下一帧继续；**坏帧不消费后续字节，不污染后续帧**         | 是        |

每个坏帧都通过 `EventHandler` 回调（`ParseError{Kind, Detail, RawByte}`），
`Stats{GoodFrames, BadFrames, Resyncs}` 提供可观察的恢复统计——
连续异常触发的是“可见的重同步与事件流”，而非静默丢弃。

## 2.3 为什么坏帧不可能污染后续帧

解析器维护单一内部缓冲；任何一步发现错误时，被丢弃的字节区间都是确定的：
CRC/长度错误按帧边界（或下一个 magic）丢弃，magic 错误按字节丢弃。
好帧在回调前会**拷贝** payload，与内部缓冲解除别名。

## 3. 数据可用性与健康度（`sensor` + `driver`）

### 3.1 读数判定（每设备有状态）

按顺序：

1. **取值域**：`value ∉ [Spec.Min, Spec.Max]` → `VerdictOutOfRange`，拒绝。
2. **时效性**：`RecvTS − DeviceTS > MaxAge`（过旧）或 `DeviceTS` 超前过多 →
   `VerdictStale`，拒绝。
3. **乱序**：`Spec.AllowReorder=false` 且 `DeviceTS < 上一条已接受读数` →
   `VerdictOutOfOrder`，拒绝。
4. 否则接受为 `VerdictOK`，并推进时间戳基线。

被拒读数**不推进基线**，因此一条乱序/坏读数不会影响后续判定。

### 3.2 健康度状态机

```
每次(重)连接 ─► Degraded（观察期，清空旧状态）
                    │ 连续 ProbationGood 条被接受读数
                    ▼
                 Healthy ──坏帧率 ≥ MaxBadRate（滑窗 BadWindow）──► Degraded
                    ▲                                                    │
                    │                                                    │
                 观察期通过                              连续 MaxStalls 次空闲超时
                    ▲                                                    │
                    └────────重连成功──────── Isolated ◄──连接读/写错误──┘
```

- **Healthy**：正常采集，`GetReading` 直接返回最新读数。
- **Degraded**：观察期或间歇坏帧；仍可返回读数，但读数超过 `Spec.MaxAge`
  时返回 `ErrStaleReading`，不下发过期数据。
- **Isolated**：设备疑似阻塞或连接故障；`GetReading` 返回
  `ErrDeviceUnavailable`，清空读数队列与判定基线，按指数退避
  （`Initial × Factor`，上限 `Max`）重连。

### 3.3 恢复不变量：恢复后绝不沿用故障前状态

每次（重）连接前 `runner.resetBeforeConnect()`：

- `sensor.Validator.Reset()`：时间戳基线清空，不会把新连接的数据误判成乱序；
- 读数队列排空：`Drain` / `GetReading` 不会返回故障前的旧读数；
- 滑窗质量、连续好帧/连续 stall 计数归零；
- 重新进入 Degraded 观察期，必须再次凭连续好读数升级 Healthy。

### 3.4 并发隔离

- 每台设备一个 supervisor goroutine + 一条独立有界队列（默认 64）。
- 队列满时丢最旧读数，**绝不阻塞采集循环**；事件通道满时丢事件并告警，
  绝不阻塞会话回调。
- 单设备的拨号、读超时、阻塞与退避完全独立：A 设备阻塞只会隔离 A，
  B 设备的采集与 `GetReading` 不受影响（见集成测试
  `TestManagerBlockedDeviceIsolationDoesNotAffectOthers`）。
- supervisor 与读循环均 `recover` 兜底 panic。

## 4. 模拟设备端（`sim` 包）

`sim.Device` 用 `net.Pipe` 在内存中模拟链路，`Behavior(attempt)` 按拨号序号
返回脚本，`Step` 支持：

- `StepSendFrame`（可 `Split` 制造半包、`PackWithNext` 制造粘包）；
- `StepSendRaw`（任意坏字节，如 CRC 损坏帧、垃圾 magic）；
- `StepSleep` / `StepBlock`（静默，模拟设备无响应）/ `StepClose`（主动断链）。

`Attempts()` 可用于断言重连次数。模拟设备端不监听任何端口，不依赖外部服务。

## 5. 本地验证方法

```bash
go vet ./...
go test ./...                 # 快速
go test -race ./...           # 竞态
go test ./... -count=20       # 稳定性（无 flake）
go run ./cmd/demo             # 观察状态迁移日志
```

### 测试矩阵

| 需求点                       | 覆盖测试 |
|------------------------------|----------|
| 半包/粘包                     | `frame.TestHalfPacket`、`TestStickyPackets`、`driver.TestManagerHalfAndStickyFrames`、`sim.TestHalfPacketWritesArriveInTwoPieces`、`TestStickyPacketsArriveInOneWrite` |
| 坏帧不污染后续帧/重同步        | `frame.TestBadFrameDoesNotPolluteNext`、`TestLengthTooBig`、`session.TestReadLoopParsesFramesAndReportsBadFrames` |
| 重复启动/重入关闭/未连接读写   | `session.TestConnectLifecycleAndRejectRepeatedStart`、`TestCloseIdempotentAndRejectsFurtherOps`、`TestReadWriteWhenNotConnected` |
| 拨号失败/断开后重连            | `session.TestDialFailureLeavesDisconnected`、`TestReconnectAfterServerClose` |
| 乱序/越界/过期读数            | `sensor/*`、`driver.TestManagerOutOfRangeAndOutOfOrder`、`TestManagerStaleTimestampRejected` |
| 单设备阻塞隔离不影响他人       | `driver.TestManagerBlockedDeviceIsolationDoesNotAffectOthers` |
| 故障恢复不沿用旧状态           | `driver.TestManagerRecoveryAfterDisconnect` |
| 坏帧率降级                    | `driver.TestManagerBadFramesAndDegrade` |

### 日志可观察性

`logging` 输出统一 UTC 时间戳与排序后的 key=value，关键输入、状态迁移与
判定依据均有记录：`session state`、`health transition`（含 from/to/reason）、
`bad frame rejected`（含错误类别与 want/got CRC）、`reading rejected`
（含 verdict 与具体原因）、`read idle timeout`（含 stalls/limit）。
