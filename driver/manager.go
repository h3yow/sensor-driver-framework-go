// Package driver 实现多设备并发采集管理：会话编排、健康度降级/隔离、
// 重连退避、读数判定与每设备独立的数据可用性。
//
// 健康度语义：
//
//	Healthy   正常采集，直接提供最新读数；
//	Degraded  观察期（刚连接）或坏帧率超阈；仍可提供未过期的缓存读数，
//	          过期则返回 ErrStaleReading；
//	Isolated  连续空闲超时（设备阻塞）或连接故障；停止对外提供数据，
//	          清空读数队列与判定基线，按指数退避重连。
//
// 恢复规则：重连成功后进入观察期，连续收到 ProbationGood 条被接受读数
// 才恢复 Healthy；观察期前的状态（队列/时间戳基线）一律不沿用。
//
// 并发隔离：每台设备一个独立 supervisor goroutine 与独立有界队列；
// 任一设备的阻塞、超时、丢帧都只影响自身，不拖慢其它设备。
package driver

import (
	"context"
	"sync"
	"time"

	"github.com/h3yow/sensor-driver-framework-go/logging"
	"github.com/h3yow/sensor-driver-framework-go/sensor"
	"github.com/h3yow/sensor-driver-framework-go/session"
)

// Health 是设备健康度。
type Health int

const (
	Healthy  Health = iota // 正常采集
	Degraded               // 降级：观察期/间歇坏帧，仍可返回缓存读数
	Isolated               // 隔离：不可用，停止沿用旧状态并按退避重连
)

func (h Health) String() string {
	switch h {
	case Healthy:
		return "healthy"
	case Degraded:
		return "degraded"
	case Isolated:
		return "isolated"
	default:
		return "unknown"
	}
}

// DeviceConfig 是单设备配置。
type DeviceConfig struct {
	ID          string
	Kind        sensor.Kind
	Dialer      session.Dialer
	Spec        sensor.Spec
	QueueSize   int           // 每设备读数队列深度
	MaxBadRate  float64       // 窗口内坏帧率阈值(0~1)，达到则降级
	BadWindow   int           // 坏帧率统计窗口（帧数）
	MaxStalls   int           // 连续空闲超时上限，超过则隔离
	Reconnect   BackoffConfig // 重连退避
	IdleTimeout time.Duration // 读空闲超时（模拟设备阻塞检测），0 使用默认值
	// ProbationGood 为连接建立后升级为 Healthy 所需的连续好帧数，0 使用默认值 2。
	ProbationGood int
	// OnHealth 在设备健康度每次发生迁移时回调（Healthy/Degraded/Isolated），
	// 供上层做可观察的告警与恢复联动；可为 nil。回调在 supervisor goroutine
	// 中同步执行，实现方必须快速返回且不得阻塞。
	OnHealth func(h Health)
}

// BackoffConfig 是重连退避参数。
type BackoffConfig struct {
	Initial time.Duration
	Max     time.Duration
	Factor  float64
}

// DeviceStatus 是某台设备在某一时刻的可观察快照。
type DeviceStatus struct {
	ID           string
	Health       Health
	SessionState session.State
	Spec         sensor.Spec
	Attempts     int
	Counters     Counters
}

// Counters 是采集统计计数。
type Counters struct {
	GoodFrames    int64
	BadFrames     int64
	Accepted      int64
	RejectedOOO   int64 // 乱序被拒
	RejectedOOR   int64 // 越界被拒
	RejectedStale int64 // 过期/超前被拒
	IdleTimeouts  int64
	Connects      int64 // 成功建立连接的次数（含首次）
	LastGoodTS    int64 // 最近一条被接受读数的设备时间戳
}

// Clock 抽象时间源，测试中可注入可控时钟。
type Clock interface {
	Now() time.Time
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

// Manager 管理多台设备的并发采集。
type Manager struct {
	logger logging.Logger
	clock  Clock

	mu        sync.Mutex
	devices   map[string]*runner
	started   bool
	stopCh    chan struct{}
	wg        sync.WaitGroup
	rootCtx   context.Context
	cancelCtx context.CancelFunc
}

// Option 配置 Manager。
type Option func(*managerConfig)

type managerConfig struct {
	logger logging.Logger
	clock  Clock
}

// WithLogger 注入日志器。
func WithLogger(l logging.Logger) Option {
	return func(c *managerConfig) { c.logger = l }
}

// WithClock 注入时钟（测试用）。
func WithClock(clock Clock) Option {
	return func(c *managerConfig) { c.clock = clock }
}

// NewManager 创建管理器。
func NewManager(opts ...Option) *Manager {
	cfg := managerConfig{logger: logging.Nop(), clock: realClock{}}
	for _, o := range opts {
		o(&cfg)
	}
	return &Manager{
		logger:  cfg.logger,
		clock:   cfg.clock,
		devices: make(map[string]*runner),
		stopCh:  make(chan struct{}),
	}
}

// nowMs 返回当前时间的 unix 毫秒。
func (m *Manager) nowMs() int64 { return m.clock.Now().UnixMilli() }

// Register 注册一台设备（必须在 Start 前）。
func (m *Manager) Register(cfg DeviceConfig) error {
	applyDefaults(&cfg)
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.started {
		return ErrAlreadyStarted
	}
	if _, ok := m.devices[cfg.ID]; ok {
		return ErrDuplicateDevice
	}
	r := newRunner(cfg, m.logger, m.clock)
	m.devices[cfg.ID] = r
	m.logger.Info("device registered", "device", cfg.ID, "kind", cfg.Kind)
	return nil
}

func applyDefaults(cfg *DeviceConfig) {
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = 64
	}
	if cfg.BadWindow <= 0 {
		cfg.BadWindow = 20
	}
	if cfg.MaxBadRate <= 0 {
		cfg.MaxBadRate = 0.5
	}
	if cfg.MaxStalls <= 0 {
		cfg.MaxStalls = 3
	}
	if cfg.IdleTimeout <= 0 {
		cfg.IdleTimeout = 500 * time.Millisecond
	}
	if cfg.ProbationGood <= 0 {
		cfg.ProbationGood = 2
	}
	if cfg.Reconnect.Initial <= 0 {
		cfg.Reconnect.Initial = 50 * time.Millisecond
	}
	if cfg.Reconnect.Max <= 0 {
		cfg.Reconnect.Max = 2 * time.Second
	}
	if cfg.Reconnect.Factor <= 1 {
		cfg.Reconnect.Factor = 2
	}
}

// Start 启动所有已注册设备的采集 worker。重复启动返回错误。
func (m *Manager) Start(ctx context.Context) error {
	m.mu.Lock()
	if m.started {
		m.mu.Unlock()
		return ErrAlreadyStarted
	}
	m.started = true
	m.rootCtx, m.cancelCtx = context.WithCancel(ctx)
	devs := make([]*runner, 0, len(m.devices))
	for _, r := range m.devices {
		devs = append(devs, r)
	}
	m.mu.Unlock()

	for _, r := range devs {
		m.wg.Add(1)
		go func(r *runner) {
			defer m.wg.Done()
			r.supervise(m.rootCtx, m.stopCh)
		}(r)
	}
	m.logger.Info("manager started", "devices", len(devs))
	return nil
}

// Stop 停止所有设备并释放资源；可重入。
func (m *Manager) Stop() {
	m.mu.Lock()
	if !m.started {
		m.mu.Unlock()
		return
	}
	m.started = false
	close(m.stopCh)
	cancel := m.cancelCtx
	m.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	m.wg.Wait()

	m.mu.Lock()
	m.stopCh = make(chan struct{})
	m.mu.Unlock()
	m.logger.Info("manager stopped")
}

// GetReading 获取某设备最新可用读数。
//   - Healthy：返回最新缓存读数；
//   - Degraded：读数在时效窗口内可返回，否则 ErrStaleReading；
//   - Isolated/不存在：返回 ErrDeviceUnavailable / ErrDeviceNotFound。
func (m *Manager) GetReading(id string) (sensor.Reading, error) {
	m.mu.Lock()
	r, ok := m.devices[id]
	m.mu.Unlock()
	if !ok {
		return sensor.Reading{}, ErrDeviceNotFound
	}
	return r.latest(m.nowMs())
}

// Drain 非阻塞地读取某设备队列中的全部已接受读数（用于消费流）。
func (m *Manager) Drain(id string) []sensor.Reading {
	m.mu.Lock()
	r, ok := m.devices[id]
	m.mu.Unlock()
	if !ok {
		return nil
	}
	return r.drain()
}

// Status 返回所有设备的状态快照（按 ID 排序，结果稳定）。
func (m *Manager) Status() []DeviceStatus {
	m.mu.Lock()
	ids := make([]string, 0, len(m.devices))
	byID := make(map[string]*runner, len(m.devices))
	for id, r := range m.devices {
		ids = append(ids, id)
		byID[id] = r
	}
	m.mu.Unlock()
	// 排序在锁外完成即可，map 快照已固定。
	sortStrings(ids)
	out := make([]DeviceStatus, 0, len(ids))
	for _, id := range ids {
		out = append(out, byID[id].snapshot())
	}
	return out
}

// WaitForHealth 轮询等待设备进入指定健康度或超时。
// 注意：等待截止使用真实墙钟，与注入的业务时钟解耦，
// 避免冻结时钟导致无限等待。
func (m *Manager) WaitForHealth(id string, h Health, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		m.mu.Lock()
		r, ok := m.devices[id]
		m.mu.Unlock()
		if ok && r.getHealth() == h {
			return true
		}
		time.Sleep(2 * time.Millisecond)
	}
	return false
}
