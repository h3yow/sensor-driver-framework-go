# sensor-driver-framework-go

面向多传感器的采集驱动框架。用**可编程模拟设备端**（`pkg/simdev`）替代真实硬件，
使设备异常、数据乱序与并发采集下的行为可预测、可解释、可复现。全部测试本地运行，
不依赖真实硬件或外部服务。

## 架构

```
cmd/demo        演示入口：多类行为设备并发采集（含时钟漂移/尖峰/跳变）
pkg/frame       传输层分帧与容错解析
pkg/driver      单设备会话生命周期状态机
pkg/simdev      可编程模拟传感器设备端（TCP，本地回环）
pkg/timesync    单设备时钟对齐与异常防护（偏差估计、跳变重建）
pkg/acquire     多设备并发采集、可用性判定与健康度管理
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
| `ClockAhead` | 设备时间戳明显超前（启用时间对齐时，见下节） |
| `ClockBehind` | 设备时间戳明显滞后（启用时间对齐时） |
| `ClockInvalid` | 时间戳完全不合理：零值或早于 `MinValidTime`（启用时间对齐时） |
| `ClockOutlier` | 偏离对齐估计但未达超前/滞后阈值（启用时间对齐时） |

## 设备时间对齐与时钟异常防护（pkg/timesync + pkg/acquire）

真机的时钟各自为政：有静态偏移、缓慢漂移，偶尔跳变，重启后从零重计。
在 `acquire.Config` 中设置 `TimeSync: &timesync.Config{...}` 即启用
（**默认 nil 为禁用，禁用时判定语义与此前版本完全一致**）。

**对齐模型**：每台设备一个独立的 `Aligner`，以 `(到达时间 - 设备时间)` 为
偏差样本，用定长环形缓冲（默认 32 个样本，内存有界）+ 中位数估计偏差，
能跟随缓慢漂移；偏离估计超过 `AcceptWindowMs` 的样本被拒绝，个别离群
读数不会带偏估计。读数同时携带原始时间与对齐结果，便于解释：

| Reading 字段 | 含义 |
|---|---|
| `TS` | 设备原始时间戳（对齐前） |
| `AlignedTS` | 统一时间轴上的时间（`Adjusted=true` 时有效） |
| `Adjusted` | 该读数时间是否被对齐调整过 |
| `TimeState` | 当时对齐状态：`Aligning` / `Aligned` / `Degraded` |

跨设备排序、比较、聚合应使用 `Adjusted=true` 读数的 `AlignedTS`。

**时钟异常防护**：残差（样本 − 估计）超出可配置范围时给出可区分判定——
超前（`AheadMs`）、滞后（`BehindMs`）、完全不合理（`MinValidTime`，默认
1970-01-01，即仅拒绝零值/负值）。异常读数是**中性**的：不成为后续读数的
比较基准、不计入健康度，因此紧跟其后的正常读数不会被误判为乱序；
单台设备的时钟异常只影响自己的对齐器，不拖慢其它设备。

**跳变与恢复**：连续 `JumpConfirm`（默认 3）个被拒绝样本判定为时钟跳变，
自动丢弃旧偏差重建时间序；断开重连也会重置对齐与判定基线。设备重启后
从零重计（上报 uptime）会被当作一次大跳变重新对齐，健康度随连续良好
读数真正回升到 `Good`。若超过 `AlignTimeoutMs`（默认 10s）仍无法对齐，
进入 `Degraded`：每条读数按既定策略记故障（可降级/隔离），且读数标记
`TimeState=Degraded`，不应进入跨设备聚合结果，避免污染其它设备数据。

**可配置项**（`timesync.Config`，零值取默认）：

| 字段 | 默认 | 含义 |
|---|---|---|
| `MinSamples` | 3 | 建立对齐所需的一致样本数 |
| `AcceptWindowMs` | 1000 | 残差接受窗口，超出则不更新滤波 |
| `AheadMs` / `BehindMs` | 30000 | 明显超前 / 滞后阈值 |
| `JumpConfirm` | 3 | 连续被拒绝样本达到该数判定为跳变 |
| `AlignTimeoutMs` | 10000 | 超过该时长仍无法对齐则降级 |
| `SampleBufSize` | 32 | 样本环形缓冲大小（内存有界） |
| `MinValidTime` | 1970-01-01 | 绝对有效下界（拒绝零值/负值时间戳） |

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
| `TsOffsetMs` | 时间戳偏移（负值制造过期读数） |
| `TsDriftMsPerSec` | 设备时钟漂移速率（每秒快/慢多少毫秒） |
| `TsSpikeEvery N` / `TsSpikeMs` | 每 N 帧注入一条时间戳尖峰 |
| `TsJumpAfter N` / `TsJumpMs` | 第 N 帧起设备时钟跳变 |
| `RebootAfter N` | 第 N 帧后模拟重启：断开连接，时钟从零重计 |

## 本地验证

```bash
go build ./...          # 编译
go vet ./...            # 静态检查
go test ./...           # 全部单元/集成测试（纯本地，无外部依赖）
go test ./... -race -count=2   # 竞态检测 + 重复运行，验证结果稳定
go test ./pkg/acquire/ -run 'TestClock|TestSingle|TestReboot|TestUnalignable|TestCrossDevice' -v  # 时间对齐场景
go run ./cmd/demo       # 运行 5 秒演示，观察日志与汇总
```

时间对齐相关测试场景（日志可见输入行为、判定原因与结果）：

| 测试 | 场景 |
|---|---|
| `TestClockAheadMarked` | 设备时钟明显超前 → 标记 `ClockAhead` |
| `TestSingleAnomalyThenNormalNotMisjudged` | 单条异常时间戳后紧跟正常读数，不误判 |
| `TestClockJumpRealigns` | 运行中时钟跳变 → 重建时间序并恢复对齐 |
| `TestRebootReconnectRecovers` | 重启/重连后从零重计 → 重新对齐，健康回升 |
| `TestUnalignableDeviceLimited` | 长期无法对齐 → 降级并限制数据，不影响其它设备 |
| `TestCrossDeviceUnifiedTimeline` | 不同偏移设备对齐到统一时间轴，可跨设备比较 |
| `pkg/timesync` 单测 | 偏差估计、漂移跟随、离群拒绝、跳变重建、降级恢复 |

日志中打印关键输入（设备行为脚本）、状态迁移（`[driver]`）、
对齐过程（`[timesync] aligned/rejected/clock jump`）、
判定依据（`[acquire] judge=... reason=...`）与健康度迁移。
