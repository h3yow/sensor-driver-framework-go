# sensor-driver-framework-go

面向多传感器的采集驱动框架。用**可编程模拟设备端**（`pkg/simdev`）替代真实硬件，
使设备异常、数据乱序与并发采集下的行为可预测、可解释、可复现。全部测试本地运行，
不依赖真实硬件或外部服务。

## 架构

```
cmd/demo        演示入口：多类行为设备（含时钟异常设备）并发采集
pkg/frame       传输层分帧与容错解析
pkg/driver      单设备会话生命周期状态机
pkg/simdev      可编程模拟传感器设备端（TCP，本地回环）
pkg/acquire     多设备并发采集、可用性判定与健康度管理
pkg/clockalign  设备时间对齐与时钟异常防护
```

## 设备生命周期语义（pkg/driver）

确定的状态机，所有迁移均打印日志：

```
Idle -> Connecting -> Connected -> Closing -> Closed
                         ^                       |
                         +------- 重连 -----------+
```

| 操作 | 非法时的失败原因（可区分） |
|---|---|
| 重复 `Connect`（Connecting/Connected） | `ErrAlreadyStarted` |
| 未连接时 `Read`/`Write`（Idle） | `ErrNotConnected` |
| 关闭后 `Read`/`Write`/`Close`（Closed） | `ErrClosed` |
| 并发重入 `Close`（Closing） | `ErrClosing` |
| 未连接时 `Close`（Idle） | `ErrNotConnected` |
| 读超时 | `ErrTimeout` |

连接失败回到 `Idle` 可重试；`Closed` 后允许再次 `Connect`（重连）。
任何异常都不会导致驱动崩溃或状态永久卡死。

## 分帧与容错规则（pkg/frame）

帧布局：`[MAGIC 0xAA55][TYPE 1][SEQ 2][LEN 2][PAYLOAD N][CRC16 2]`，
CRC 为 CCITT-FALSE，覆盖 TYPE..PAYLOAD。

- **半包**：数据不足时缓存等待，不产生任何输出。
- **粘包**：一次喂入多帧时逐帧切出，互不干扰。
- **坏帧（CRC 失败 / 长度非法）**：仅丢弃该帧（或重同步到下一个 magic），
  **不污染后续帧**；每次丢弃打印原因与统计。
- **连续异常恢复**：连续坏帧达到阈值（默认 3）触发 `OnRecovery` 回调
  （可观察的恢复动作），而不是静默丢弃；采集层据此累计故障并降级。

## 数据可用性判定（pkg/acquire）

每条读数带 `Quality` 标记，判定依据打印日志：

| Quality | 判定规则 |
|---|---|
| `Good` | 通过全部检查 |
| `OutOfRange` | 数值越出 `[MinValue, MaxValue]` 或负载格式非法 |
| `OutOfOrder` | 时间戳早于本设备上一条良好读数（乱序） |
| `Stale` | 读数年龄超过 `StaleAfterMs`（过期） |
| `ClockAhead` | 设备时钟明显超前（启用时钟对齐后） |
| `ClockBehind` | 设备时钟明显滞后（启用时钟对齐后） |
| `ClockInvalid` | 时间戳完全不合理，超出硬边界（启用时钟对齐后） |
| `Unaligned` | 设备时钟未对齐（学习窗口内或长时间无法对齐），数据受限 |

## 设备时间对齐（pkg/clockalign）

默认**关闭**（`Clock.Enabled=false`），不启用时行为与之前完全一致。
启用后每台设备独立维护一份**固定大小**的时钟状态（EWMA 估计，无历史缓冲，
长时间运行内存有界）：

- **偏差估计**：以采集端时钟为统一时间基准，用 EWMA 估计设备时钟偏差
  （`aligned = raw + offset`），`Alpha` 控制平滑系数，能跟上缓慢漂移；
  偏离当前估计超过 `MaxFutureMs`/`MaxLagMs` 的样本被**拒绝**，不会带偏估计。
- **统一时间轴**：`Reading.TS` 为对齐后的时间，`Reading.RawTS` 为设备原始
  时间戳，`Reading.Adjusted` 标记该条是否被调整过，三者均可逐条查询；
  `Manager.ClockOffsetOf(name)` 可查询设备当前偏差估计。
  排序、比较、聚合应使用 `TS`。
- **异常判定**（彼此可区分）：对齐后以“到达时刻 − 估计偏差”推算设备应有
  时刻，读数偏离超过 `MaxFutureMs` 判 `ClockAhead`，超过 `MaxLagMs` 判
  `ClockBehind`；任何阶段超出硬边界（早于 `InvalidBeforeMs` 或晚于
  到达时刻+`HardFutureMs`）判 `ClockInvalid`。
  异常读数不更新偏差估计，也**不会成为后续读数的判定基线**，因此紧跟在
  异常后面的正常读数不会被误判为乱序。
- **跳变与恢复**：连续 `ResetThreshold` 条异常视为时钟跳变/重启重新计时，
  自动清空偏差估计与判定基线并就地重新学习；若新纪元早于硬下界
  （设备从零重新计时），自动下调该设备的硬下界以接受新纪元。
  每次（重）连成功也会重置该设备的时钟状态。恢复后正常累计良好读数，
  健康度可真正回升到 `Good`，不会卡在降级/隔离。
- **学习窗口与降级**：攒够 `MinSamples` 个有效样本才建立对齐；建立前的
  读数标记 `Unaligned` 不进入结果。超过 `AlignTimeoutMs` 仍无法对齐时
  按既定策略累计故障、降级直至隔离，其数据持续被标记为 `Unaligned`/
  时钟异常，不会以 `Good` 污染其它设备的聚合结果。
- **隔离性**：时钟状态按设备独立维护，一台设备的时钟异常或卡顿不影响
  其它设备采集（`TestClockFaultDeviceDoesNotAffectOthers` 验证）。

可配置项（`acquire.Config.Clock`，零值取默认）：

| 字段 | 默认 | 含义 |
|---|---|---|
| `Enabled` | `false` | 是否启用时钟对齐与异常防护 |
| `MaxFutureMs` | 2000 | 相对估计偏差的超前阈值 |
| `MaxLagMs` | 5000 | 相对估计偏差的滞后阈值 |
| `InvalidBeforeMs` | 946684800000（2000-01-01） | 完全不合理硬下界（Unix 毫秒） |
| `HardFutureMs` | 86400000（24h） | 完全不合理硬上界（相对到达时刻） |
| `ResetThreshold` | 3 | 连续异常触发时钟重建的条数 |
| `MinSamples` | 3 | 建立对齐所需最少有效样本 |
| `Alpha` | 0.2 | 偏差估计 EWMA 平滑系数 |
| `AlignTimeoutMs` | 30000 | 超过该时长未对齐判定为无法对齐 |

## 健康度语义（pkg/acquire）

按**连续故障数**分级，故障包括：连接失败、读超时、读错误、帧流恢复回调、负载非法。

| Health | 语义 |
|---|---|
| `Good` | 正常采集 |
| `Degraded` | 连续故障 ≥ `DegradeThreshold`（默认 3）：继续采集但标记 |
| `Isolated` | 连续故障 ≥ `IsolateThreshold`（默认 6）：暂停采集，退避后重连 |

恢复策略：需连续 `RecoveryRequired`（默认 3）条良好读数才回到 `Good`，
且**恢复时重置判定基线**（`lastTS` 清零），不沿用故障前状态；
故障计数清零，再次故障需重新累计。

并发保证：每个设备一个采集 goroutine，单设备阻塞、超时或丢帧不拖慢其它设备
（由 `TestConcurrentBlockingDeviceDoesNotSlowOthers` 验证）。

## 模拟设备端行为脚本（pkg/simdev）

`Behavior` 零值为理想设备，可组合注入：

| 字段 | 效果 |
|---|---|
| `SplitFrames` | 每帧拆两段发送（半包） |
| `Sticky` | 多帧合并一次发送（粘包） |
| `CorruptEvery N` | 每 N 帧注入一个 CRC 坏帧 |
| `BlockAfter N` / `BlockMs` | 每 N 帧后阻塞指定毫秒（设备卡死） |
| `ReorderWindow N` | 窗口内反转发送顺序（时间戳乱序） |
| `TsOffsetMs` | 时间戳偏移（负值制造过期读数，正值制造时钟偏快） |
| `TsJumpAfter N` / `TsJumpMs` | 第 N 帧后时间戳跳变指定毫秒（运行中时钟跳变） |
| `TsFromZero` | 时间戳从本次启动起算（模拟重启后从零计时） |

## 本地验证

```bash
go build ./...          # 编译
go vet ./...            # 静态检查
go test ./...           # 全部单元/集成测试（纯本地，无外部依赖）
go test ./... -race -count=2   # 竞态检测 + 重复运行，验证结果稳定
go test ./pkg/clockalign/ ./pkg/acquire/ -run 'Clock|Epoch|Unaligned|Jump' -v  # 时钟对齐与异常防护场景
go run ./cmd/demo       # 运行 5 秒演示，观察日志与汇总
```

日志中打印关键输入（设备行为脚本）、状态迁移（`[driver]`）、
判定依据（`[acquire] judge=... reason=...`）与健康度迁移。
