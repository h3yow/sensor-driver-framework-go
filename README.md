# 多传感器采集驱动框架（Go）

一套面向多传感器的采集驱动框架。传输对端由**可编程的模拟设备端**承担，
不依赖真实硬件或外部服务，使设备异常、数据乱序与并发采集下的行为
可预测、可观察、可复现。

仅依赖 Go 标准库；Go >= 1.25。

## 包结构

| 包        | 职责                                                                 |
|-----------|----------------------------------------------------------------------|
| `frame`   | 二进制分帧、CRC16 校验、流式半包/粘包/坏帧容错解析                    |
| `session` | 单设备会话与确定的生命周期状态机（连接/断开/重连/关闭）               |
| `sensor`  | 统一读数模型、取值域校验、过期/乱序判定、故障后状态重置               |
| `driver`  | 多设备并发采集编排：健康度降级/隔离、指数退避重连、每设备独立队列     |
| `sim`     | 可编程模拟设备端（内存 `net.Pipe`）：半包、粘包、坏帧、阻塞、断链     |
| `logging` | 结构化文本日志（key=value，键排序，输出稳定）                         |
| `cmd/demo`| 纯本地演示：三台设备覆盖半包/粘包、坏帧降级、阻塞隔离与恢复          |

## 快速开始

```bash
# 运行全部本地测试（不访问网络/硬件）
go test ./...

# 带竞态检测
go test -race ./...

# 运行演示（3 秒后自动退出）
go run ./cmd/demo
```

## 最小用法

```go
m := driver.NewManager(driver.WithLogger(logger))

m.Register(driver.DeviceConfig{
    ID:     "temp-1",
    Kind:   sensor.KindTemperature,
    Dialer: device.Dialer(),           // 模拟设备；真实硬件可替换为 net.Dial
    Spec: sensor.Spec{
        Kind: sensor.KindTemperature, Min: -40, Max: 80,
        MaxAge: 2 * time.Second,       // 设备时间戳允许偏差
    },
    IdleTimeout: 500 * time.Millisecond,
    MaxStalls:   3,                    // 连续 3 次空闲超时 -> 隔离
    OnHealth: func(h driver.Health) {  // 健康度迁移可观察
        log.Printf("health=%s", h)
    },
})

m.Start(context.Background())
defer m.Stop()

r, err := m.GetReading("temp-1")       // Healthy 直接返回；Degraded 仅返回未过期读数
for _, st := range m.Status() { ... }  // 状态/计数快照
```

## 文档

- [docs/design.md](docs/design.md)：设备生命周期与健康度语义、分帧与容错规则、
  故障恢复不变量、本地验证方法。

## 稳定性

- 全部用例可重复运行，使用与墙钟对齐的可控时钟与脚本化设备，结果确定。
- CI 本地命令：`go vet ./... && go test -race -count=20 ./...`
