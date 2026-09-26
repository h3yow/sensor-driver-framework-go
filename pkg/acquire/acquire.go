// Package acquire 实现多设备并发采集、数据可用性判定与健康度管理。
//
// 每个设备一个采集 goroutine，单设备阻塞/超时/丢帧不影响其它设备。
// 健康度按连续故障数降级（Degraded）或隔离（Isolated），
// 恢复需连续若干条良好读数，且恢复后重置判定基线，不沿用故障前状态。
package acquire

import (
	"errors"
	"log"
	"sync"
	"time"

	"github.com/h3yow/sensor-driver-framework-go/pkg/driver"
	"github.com/h3yow/sensor-driver-framework-go/pkg/frame"
	"github.com/h3yow/sensor-driver-framework-go/pkg/simdev"
)

// Quality 标记读数可用性。
type Quality int

const (
	QualityGood       Quality = iota
	QualityOutOfOrder         // 时间戳乱序
	QualityOutOfRange         // 数值越界
	QualityStale              // 过期读数
)

func (q Quality) String() string {
	switch q {
	case QualityGood:
		return "Good"
	case QualityOutOfOrder:
		return "OutOfOrder"
	case QualityOutOfRange:
		return "OutOfRange"
	case QualityStale:
		return "Stale"
	}
	return "Unknown"
}

// Reading 是一条带判定结果的采集读数。
type Reading struct {
	Device  string
	Value   float64
	TS      time.Time
	Quality Quality
}

// Health 表示设备健康等级。
type Health int

const (
	HealthGood Health = iota
	HealthDegraded
	HealthIsolated
)

func (h Health) String() string {
	switch h {
	case HealthGood:
		return "Good"
	case HealthDegraded:
		return "Degraded"
	case HealthIsolated:
		return "Isolated"
	}
	return "Unknown"
}

// Config 是采集与判定参数。
type Config struct {
	ReadTimeoutMs    int     // 单次读超时（默认 200ms）
	StaleAfterMs     int     // 读数超过该时长视为过期（默认 1000ms）
	MinValue         float64 // 合法数值下界（默认 -50）
	MaxValue         float64 // 合法数值上界（默认 150）
	DegradeThreshold int     // 连续故障降级阈值（默认 3）
	IsolateThreshold int     // 连续故障隔离阈值（默认 6）
	RecoveryRequired int     // 恢复所需连续良好读数（默认 3）
	RetryBackoffMs   int     // 故障重连退避（默认 100ms）
}

func (c *Config) withDefaults() Config {
	out := *c
	if out.ReadTimeoutMs <= 0 {
		out.ReadTimeoutMs = 200
	}
	if out.StaleAfterMs <= 0 {
		out.StaleAfterMs = 1000
	}
	if out.MaxValue == 0 && out.MinValue == 0 {
		out.MinValue, out.MaxValue = -50, 150
	}
	if out.DegradeThreshold <= 0 {
		out.DegradeThreshold = 3
	}
	if out.IsolateThreshold <= 0 {
		out.IsolateThreshold = 6
	}
	if out.RecoveryRequired <= 0 {
		out.RecoveryRequired = 3
	}
	if out.RetryBackoffMs <= 0 {
		out.RetryBackoffMs = 100
	}
	return out
}

// devState 是单设备的采集状态。
type devState struct {
	name string
	addr string

	mu          sync.Mutex
	health      Health
	consecFault int
	consecGood  int
	lastTS      time.Time // 判定基线：上一条良好读数的时间戳
	hasBase     bool
}

// Manager 并发采集多个设备。
type Manager struct {
	cfg  Config
	mu   sync.RWMutex
	devs map[string]*devState
}

// NewManager 创建采集管理器（使用默认配置）。
func NewManager() *Manager { return NewManagerWithConfig(Config{}) }

// NewManagerWithConfig 使用指定配置创建采集管理器。
func NewManagerWithConfig(cfg Config) *Manager {
	c := cfg.withDefaults()
	return &Manager{cfg: c, devs: make(map[string]*devState)}
}

// Add 注册一个设备地址。
func (m *Manager) Add(name, addr string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.devs[name] = &devState{name: name, addr: addr, health: HealthGood}
}

// HealthOf 返回设备当前健康等级。
func (m *Manager) HealthOf(name string) Health {
	m.mu.RLock()
	d, ok := m.devs[name]
	m.mu.RUnlock()
	if !ok {
		return HealthIsolated
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.health
}

// Run 启动并发采集，读数投递到 out，直到 stop 关闭后返回。
func (m *Manager) Run(stop <-chan struct{}, out chan<- Reading) {
	m.mu.RLock()
	devs := make([]*devState, 0, len(m.devs))
	for _, d := range m.devs {
		devs = append(devs, d)
	}
	m.mu.RUnlock()

	var wg sync.WaitGroup
	for _, d := range devs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.loop(d, stop, out)
		}()
	}
	wg.Wait()
}

// loop 是单设备采集主循环：连接 -> 采集 -> 故障退避重连。
func (m *Manager) loop(d *devState, stop <-chan struct{}, out chan<- Reading) {
	backoff := time.Duration(m.cfg.RetryBackoffMs) * time.Millisecond
	for {
		select {
		case <-stop:
			return
		default:
		}
		sess := driver.New(d.addr)
		if err := sess.Connect(); err != nil {
			log.Printf("[acquire] dev=%s connect failed: %v", d.name, err)
			m.onFault(d, "connect: "+err.Error())
			if !sleepOr(stop, backoff) {
				return
			}
			continue
		}
		m.collect(d, sess, stop, out)
		_ = sess.Close()
		// 采集循环因故障退出：退避后重连
		if !sleepOr(stop, backoff) {
			return
		}
	}
}

// collect 在一条已建立连接上持续采集，直到连接故障或 stop。
func (m *Manager) collect(d *devState, sess *driver.Session, stop <-chan struct{}, out chan<- Reading) {
	dec := frame.NewDecoder(m.cfg.DegradeThreshold)
	dec.OnRecovery = func(s frame.Stats) {
		log.Printf("[acquire] dev=%s frame stream recovery triggered, stats=%+v", d.name, s)
		m.onFault(d, "frame stream recovery")
	}
	for {
		select {
		case <-stop:
			return
		default:
		}
		if m.HealthOf(d.name) == HealthIsolated {
			// 隔离：暂停采集，等待退避后由外层重连
			log.Printf("[acquire] dev=%s isolated, pausing acquisition", d.name)
			return
		}
		raw, err := sess.Read(m.cfg.ReadTimeoutMs)
		if err != nil {
			if errors.Is(err, driver.ErrTimeout) {
				m.onFault(d, "read timeout")
				continue // 超时不算连接故障，继续读
			}
			log.Printf("[acquire] dev=%s read error: %v, reconnecting", d.name, err)
			m.onFault(d, "read: "+err.Error())
			return
		}
		for _, f := range dec.Feed(raw) {
			r, ok := m.judge(d, f)
			if ok {
				m.onGood(d)
			}
			select {
			case out <- r:
			case <-stop:
				return
			}
		}
	}
}

// judge 对一帧进行可用性判定，返回读数；ok 表示该读数计入健康度良好统计。
func (m *Manager) judge(d *devState, f frame.Frame) (Reading, bool) {
	ts, value, valid := simdev.DecodePayload(f.Payload)
	r := Reading{Device: d.name, Value: value, TS: ts, Quality: QualityGood}
	if !valid {
		r.Quality = QualityOutOfRange
		log.Printf("[acquire] dev=%s seq=%d judge=OutOfRange reason=malformed payload len=%d", d.name, f.Seq, len(f.Payload))
		m.onFault(d, "malformed payload")
		return r, false
	}
	now := time.Now()
	d.mu.Lock()
	defer d.mu.Unlock()
	switch {
	case value < m.cfg.MinValue || value > m.cfg.MaxValue:
		r.Quality = QualityOutOfRange
		log.Printf("[acquire] dev=%s seq=%d judge=OutOfRange reason=value %.2f outside [%.2f, %.2f]",
			d.name, f.Seq, value, m.cfg.MinValue, m.cfg.MaxValue)
	case d.hasBase && ts.Before(d.lastTS):
		r.Quality = QualityOutOfOrder
		log.Printf("[acquire] dev=%s seq=%d judge=OutOfOrder reason=ts %s before last %s",
			d.name, f.Seq, ts.Format(time.RFC3339Nano), d.lastTS.Format(time.RFC3339Nano))
	case now.Sub(ts) > time.Duration(m.cfg.StaleAfterMs)*time.Millisecond:
		r.Quality = QualityStale
		log.Printf("[acquire] dev=%s seq=%d judge=Stale reason=age %v > %dms",
			d.name, f.Seq, now.Sub(ts), m.cfg.StaleAfterMs)
	default:
		d.lastTS = ts
		d.hasBase = true
	}
	return r, r.Quality == QualityGood
}

// onGood 记录一条良好读数，并按策略恢复健康度。
func (m *Manager) onGood(d *devState) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.consecFault = 0
	d.consecGood++
	if d.health != HealthGood && d.consecGood >= m.cfg.RecoveryRequired {
		log.Printf("[acquire] dev=%s health %s -> Good after %d consecutive good readings; baseline reset",
			d.name, d.health, d.consecGood)
		d.health = HealthGood
		// 恢复后不得沿用故障前状态：重置判定基线
		d.hasBase = false
		d.lastTS = time.Time{}
	}
}

// onFault 记录一次故障，并按阈值降级或隔离。
func (m *Manager) onFault(d *devState, reason string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.consecGood = 0
	d.consecFault++
	old := d.health
	switch {
	case d.consecFault >= m.cfg.IsolateThreshold:
		d.health = HealthIsolated
	case d.consecFault >= m.cfg.DegradeThreshold:
		d.health = HealthDegraded
	}
	if d.health != old {
		log.Printf("[acquire] dev=%s health %s -> %s (consecFault=%d, reason=%s)",
			d.name, old, d.health, d.consecFault, reason)
	} else {
		log.Printf("[acquire] dev=%s fault recorded (consecFault=%d, reason=%s)", d.name, d.consecFault, reason)
	}
}

// sleepOr 睡眠 d 或直到 stop 关闭；返回 false 表示应停止。
func sleepOr(stop <-chan struct{}, d time.Duration) bool {
	select {
	case <-stop:
		return false
	case <-time.After(d):
		return true
	}
}
