package driver

import (
	"context"
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/h3yow/sensor-driver-framework-go/frame"
	"github.com/h3yow/sensor-driver-framework-go/logging"
	"github.com/h3yow/sensor-driver-framework-go/sensor"
	"github.com/h3yow/sensor-driver-framework-go/session"
)

func init() {
	// 保持 sortStrings 单一实现，避免在多处引入排序逻辑。
}

func sortStrings(s []string) { sort.Strings(s) }

// runner 是单台设备的全部运行时状态。除队列/快照字段外，
// 健康度与统计的业务流转只发生在 supervisor goroutine 中。
type runner struct {
	cfg    DeviceConfig
	logger logging.Logger
	clock  Clock

	sess   *session.Session
	events chan session.Event

	mu        sync.RWMutex
	health    Health
	sessState session.State
	last      sensor.Reading
	hasLast   bool
	queue     chan sensor.Reading

	counters     Counters
	stalls       int
	goodStreak   int
	recentFrames []bool // true=好帧，false=坏帧（滑动窗口）
	validator    *sensor.Validator
}

func newRunner(cfg DeviceConfig, l logging.Logger, clock Clock) *runner {
	r := &runner{
		cfg:       cfg,
		logger:    l,
		clock:     clock,
		health:    Isolated, // 启动前视为隔离/不可用
		sessState: session.StateIdle,
		queue:     make(chan sensor.Reading, cfg.QueueSize),
		validator: sensor.NewValidator(cfg.Spec),
	}
	return r
}

// supervise 是单设备的生命周期总循环：连接 -> 事件处理 -> 故障隔离 -> 退避重连。
// 该 goroutine 退出即代表设备被 Stop；任何内部 panic 都会被兜住，
// 避免单台设备异常拖垮整个进程。
func (r *runner) supervise(ctx context.Context, stopCh <-chan struct{}) {
	defer func() {
		if rec := recover(); rec != nil {
			r.logger.Error("supervisor panic recovered", "device", r.cfg.ID, "panic", rec)
		}
		if r.sess != nil {
			_ = r.sess.Close()
		}
	}()

	backoff := r.cfg.Reconnect.Initial
	for {
		// 每次尝试都是“干净起步”：新会话、新判定基线、空队列。
		r.resetBeforeConnect()
		r.events = make(chan session.Event, 256)
		sess := session.New(r.cfg.Dialer, session.Options{
			ReadIdle:     r.cfg.IdleTimeout,
			WriteTimeout: 2 * time.Second,
			Hooks: session.Hooks{OnEvent: func(ev session.Event) {
				// 非阻塞投递：通道满说明 supervisor 已异常缓慢，
				// 绝不允许设备回调路径被堵死。
				select {
				case r.events <- ev:
				default:
					r.logger.Warn("event dropped (supervisor busy)", "device", r.cfg.ID, "kind", ev.Kind)
				}
			}},
		})
		r.sess = sess

		if err := sess.Connect(ctx); err != nil {
			r.logger.Warn("connect failed", "device", r.cfg.ID, "reason", err.Error())
			if errors.Is(err, session.ErrClosed) || ctx.Err() != nil {
				return
			}
			if !r.waitRetry(ctx, stopCh, backoff) {
				return
			}
			backoff = nextBackoff(backoff, r.cfg.Reconnect)
			continue
		}
		atomic.AddInt64(&r.counters.Connects, 1)
		// 连接成功后退避归零，使下一次断线后的首次重连仍是快速重试。
		backoff = r.cfg.Reconnect.Initial
		r.setHealth(Degraded, "connected; entering probation")
		r.logger.Info("connected", "device", r.cfg.ID, "attempt", atomic.LoadInt64(&r.counters.Connects))

		terminated := r.pump(ctx, stopCh, sess)
		_ = sess.Close()
		if terminated {
			return
		}

		// 连接中断：隔离 -> 退避重连。
		r.setHealth(Isolated, "connection lost; isolate before reconnect")
		backoff = nextBackoff(backoff, r.cfg.Reconnect)
		if !r.waitRetry(ctx, stopCh, backoff) {
			return
		}
	}
}

// pump 处理一条连接上的全部事件，直到连接终止、被隔离或整体停止。
// 返回 true 表示 supervisor 应整体退出（Stop / 根上下文取消）。
func (r *runner) pump(ctx context.Context, stopCh <-chan struct{}, sess *session.Session) bool {
	for {
		select {
		case <-stopCh:
			return true
		case <-ctx.Done():
			return true
		case ev := <-r.events:
			stop := r.handleEvent(ev, sess)
			if stop {
				return true
			}
			// 连接已死（读/写错误或对端关闭）：交由外层隔离并重连。
			if sess.State() == session.StateDisconnected {
				return false
			}
			if r.getHealth() == Isolated {
				// 隔离由本循环在故障/阻塞时设置，交由外层重连。
				return false
			}
		}
	}
}

func (r *runner) handleEvent(ev session.Event, sess *session.Session) (stop bool) {
	switch ev.Kind {
	case session.EvStateChanged:
		r.mu.Lock()
		r.sessState = ev.State
		r.mu.Unlock()
		r.logger.Info("session state", "device", r.cfg.ID, "state", ev.State.String())
		if ev.State == session.StateClosed {
			return true
		}
	case session.EvGoodFrame:
		atomic.AddInt64(&r.counters.GoodFrames, 1)
		r.stalls = 0
		r.noteFrameQuality(true)
		r.consumeFrame(ev.Frame)
	case session.EvBadFrame:
		atomic.AddInt64(&r.counters.BadFrames, 1)
		r.noteFrameQuality(false)
		r.logger.Warn("bad frame rejected",
			"device", r.cfg.ID, "kind", ev.Bad.Kind, "detail", ev.Bad.Detail)
		r.evaluateHealth("bad frame rate over threshold")
	case session.EvIdleTimeout:
		atomic.AddInt64(&r.counters.IdleTimeouts, 1)
		r.stalls++
		r.logger.Warn("read idle timeout",
			"device", r.cfg.ID, "stalls", r.stalls, "limit", r.cfg.MaxStalls)
		if r.stalls >= r.cfg.MaxStalls {
			// 设备疑似阻塞：隔离并主动断开，触发外层重连。
			r.setHealth(Isolated, "stall limit reached; device suspected blocked")
			_ = sess.Close()
		} else {
			r.evaluateHealth("intermittent timeouts")
		}
	case session.EvReadError:
		r.logger.Warn("read error", "device", r.cfg.ID, "detail", ev.Detail)
	}
	return false
}

// noteFrameQuality 维护最近窗口内的帧质量并按坏帧率降级。
func (r *runner) noteFrameQuality(good bool) {
	r.recentFrames = append(r.recentFrames, good)
	if len(r.recentFrames) > r.cfg.BadWindow {
		r.recentFrames = r.recentFrames[1:]
	}
	if r.getHealth() != Isolated {
		r.evaluateHealth("sliding-window quality check")
	}
}

func (r *runner) evaluateHealth(reason string) {
	if len(r.recentFrames) < r.cfg.BadWindow {
		// 窗口未满不下结论，避免启动初期抖动；观察期保持 Degraded。
		return
	}
	bad := 0
	for _, g := range r.recentFrames {
		if !g {
			bad++
		}
	}
	rate := float64(bad) / float64(len(r.recentFrames))
	if rate >= r.cfg.MaxBadRate && r.getHealth() == Healthy {
		r.setHealth(Degraded, reason)
	}
}

// consumeFrame 将一帧读数转换为 Reading 并完成取值域/时效性/乱序判定。
func (r *runner) consumeFrame(f *frame.Frame) {
	if f.Type != frame.TypeReading {
		return
	}
	value, err := sensor.DecodeReadingPayload(f.Payload)
	if err != nil {
		atomic.AddInt64(&r.counters.BadFrames, 1)
		r.logger.Warn("unreadable reading payload", "device", r.cfg.ID, "seq", f.Seq)
		return
	}
	reading := sensor.Reading{
		DeviceID: r.cfg.ID,
		Kind:     r.cfg.Kind,
		Value:    value,
		Seq:      f.Seq,
		DeviceTS: f.TS,
		RecvTS:   r.clock.Now().UnixMilli(),
	}
	reading, accepted := r.validator.Judge(reading)
	if !accepted {
		switch reading.Verdict {
		case sensor.VerdictOutOfRange:
			atomic.AddInt64(&r.counters.RejectedOOR, 1)
		case sensor.VerdictOutOfOrder:
			atomic.AddInt64(&r.counters.RejectedOOO, 1)
		case sensor.VerdictStale:
			atomic.AddInt64(&r.counters.RejectedStale, 1)
		}
		r.logger.Warn("reading rejected",
			"device", r.cfg.ID, "seq", f.Seq, "verdict", reading.Verdict.String(),
			"reason", reading.RejectReason, "value", reading.Value)
		return
	}

	atomic.AddInt64(&r.counters.Accepted, 1)
	atomic.StoreInt64(&r.counters.LastGoodTS, reading.DeviceTS)
	r.goodStreak++
	r.logger.Debug("reading accepted",
		"device", r.cfg.ID, "seq", f.Seq, "value", reading.Value, "ts", reading.DeviceTS)

	// 健康度升级：观察期内连续 N 条被接受读数 -> Healthy。
	// 被接受读数（非好帧）是更强的可用性信号。
	if r.getHealth() == Degraded && r.goodStreak >= r.cfg.ProbationGood {
		r.setHealth(Healthy, "probation passed with consecutive good readings")
	}

	r.mu.Lock()
	r.last = reading
	r.hasLast = true
	r.mu.Unlock()

	// 有界非阻塞入队：消费者来不及处理时丢弃最旧读数，绝不阻塞采集循环。
	select {
	case r.queue <- reading:
	default:
		select {
		case <-r.queue:
		default:
		}
		r.queue <- reading
	}
}

// resetBeforeConnect 在每次（重）连接前清空一切故障前状态。
func (r *runner) resetBeforeConnect() {
	r.validator.Reset()
	r.stalls = 0
	r.goodStreak = 0
	r.recentFrames = nil
	// 排空旧队列，恢复后不沿用故障前读数。
drainLoop:
	for {
		select {
		case <-r.queue:
		default:
			break drainLoop
		}
	}
	r.mu.Lock()
	r.last = sensor.Reading{}
	r.hasLast = false
	r.sessState = session.StateIdle
	r.mu.Unlock()
	r.setHealth(Isolated, "reset before (re)connect; stale state discarded")
}

func (r *runner) setHealth(h Health, reason string) {
	r.mu.Lock()
	old := r.health
	r.health = h
	r.mu.Unlock()
	if old == h {
		return
	}
	r.logger.Info("health transition",
		"device", r.cfg.ID, "from", old.String(), "to", h.String(), "reason", reason)
	if r.cfg.OnHealth != nil {
		r.cfg.OnHealth(h)
	}
}

func (r *runner) getHealth() Health {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.health
}

// waitRetry 等待退避时间；返回 false 表示需要整体退出。
func (r *runner) waitRetry(ctx context.Context, stopCh <-chan struct{}, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-stopCh:
		return false
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func nextBackoff(cur time.Duration, cfg BackoffConfig) time.Duration {
	nxt := time.Duration(float64(cur) * cfg.Factor)
	if nxt > cfg.Max || nxt <= 0 {
		return cfg.Max
	}
	return nxt
}

// latest 返回最新可用读数（见 Manager.GetReading 的语义）。
func (r *runner) latest(nowMs int64) (sensor.Reading, error) {
	r.mu.RLock()
	h := r.getHealth()
	rd := r.last
	has := r.hasLast
	r.mu.RUnlock()

	switch h {
	case Isolated:
		return sensor.Reading{}, ErrDeviceUnavailable
	case Degraded:
		if !has {
			return sensor.Reading{}, ErrStaleReading
		}
		if !sensor.IsUsable(rd, nowMs, r.cfg.Spec) {
			return sensor.Reading{}, ErrStaleReading
		}
	case Healthy:
		if !has {
			return sensor.Reading{}, ErrStaleReading
		}
	}
	return rd, nil
}

// drain 非阻塞取出队列中全部已接受读数。
func (r *runner) drain() []sensor.Reading {
	out := make([]sensor.Reading, 0)
	for {
		select {
		case rd := <-r.queue:
			out = append(out, rd)
		default:
			return out
		}
	}
}

func (r *runner) snapshot() DeviceStatus {
	r.mu.RLock()
	h := r.getHealth()
	st := r.sessState
	r.mu.RUnlock()
	return DeviceStatus{
		ID:           r.cfg.ID,
		Health:       h,
		SessionState: st,
		Spec:         r.cfg.Spec,
		Attempts:     int(atomic.LoadInt64(&r.counters.Connects)),
		Counters:     loadCounters(&r.counters),
	}
}

// loadCounters 原子读取全部计数，保证快照并发安全。
func loadCounters(c *Counters) Counters {
	return Counters{
		GoodFrames:    atomic.LoadInt64(&c.GoodFrames),
		BadFrames:     atomic.LoadInt64(&c.BadFrames),
		Accepted:      atomic.LoadInt64(&c.Accepted),
		RejectedOOO:   atomic.LoadInt64(&c.RejectedOOO),
		RejectedOOR:   atomic.LoadInt64(&c.RejectedOOR),
		RejectedStale: atomic.LoadInt64(&c.RejectedStale),
		IdleTimeouts:  atomic.LoadInt64(&c.IdleTimeouts),
		Connects:      atomic.LoadInt64(&c.Connects),
		LastGoodTS:    atomic.LoadInt64(&c.LastGoodTS),
	}
}
