package driver

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/h3yow/sensor-driver-framework-go/frame"
	"github.com/h3yow/sensor-driver-framework-go/logging"
	"github.com/h3yow/sensor-driver-framework-go/sensor"
	"github.com/h3yow/sensor-driver-framework-go/sim"
)

// fakeClock 是测试用可控时钟，毫秒步进，永不自动流逝。
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

// newFakeClock 初始时刻与真实墙钟对齐：这样脚本构造时刻与帧送达时刻
// 与 RecvTS 处于同一时间轴，不产生虚假的 stale 判定；需要老化场景时
// 再显式 Advance。
func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Now()}
}
func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}
func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

// readingStep 构造一条读数帧脚本步，时间戳取自注入时钟，与驱动侧
// RecvTS 共用同一时间轴，保证行为可复现。
func readingStep(seq uint16, clock *fakeClock, offsetMs int64, value float64) sim.Step {
	ts := clock.Now().UnixMilli() + offsetMs
	return sim.Step{
		Kind:  sim.StepSendFrame,
		Frame: frame.Frame{Type: frame.TypeReading, Seq: seq, TS: ts, Payload: sensor.EncodeReadingPayload(value)},
	}
}

func rawStep(b []byte) sim.Step { return sim.Step{Kind: sim.StepSendRaw, Raw: b} }

func tempSpec() sensor.Spec {
	return sensor.Spec{Kind: sensor.KindTemperature, Min: -40, Max: 80, MaxAge: 2000 * time.Millisecond}
}

func testManager(t *testing.T, clock *fakeClock) *Manager {
	t.Helper()
	var buf bytes.Buffer
	logger := logging.New(logging.Options{Writer: &buf, Prefix: "test", Debug: true})
	m := NewManager(WithClock(clock), WithLogger(logger))
	t.Cleanup(m.Stop)
	return m
}

func fastReconnect() BackoffConfig {
	return BackoffConfig{Initial: 20 * time.Millisecond, Max: 100 * time.Millisecond, Factor: 2}
}

// healthTracer 记录设备健康度迁移轨迹，用于断言瞬时状态，避免轮询竞态。
type healthTracer struct {
	mu          sync.Mutex
	trans       []Health
	badObserved int64
}

func newHealthTracer() *healthTracer { return &healthTracer{} }

func (t *healthTracer) callback(h Health) {
	t.mu.Lock()
	t.trans = append(t.trans, h)
	t.mu.Unlock()
}
func (t *healthTracer) sawHealthy() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, h := range t.trans {
		if h == Healthy {
			return true
		}
	}
	return false
}
func (t *healthTracer) transitions() []Health {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]Health, len(t.trans))
	copy(out, t.trans)
	return out
}
func (t *healthTracer) current() Health {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.trans) == 0 {
		return Isolated
	}
	return t.trans[len(t.trans)-1]
}

func waitCondition(timeout time.Duration, fn func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return true
		}
		time.Sleep(2 * time.Millisecond)
	}
	return fn()
}

// 场景1：正常采集，经历观察期后变 Healthy，读数可获取。
func TestManagerHealthyCollection(t *testing.T) {
	clock := newFakeClock()
	var seq uint16
	dev := sim.New(func(attempt int) sim.Script {
		var steps []sim.Step
		for i := 0; i < 5; i++ {
			seq++
			steps = append(steps, readingStep(seq, clock, 0, 25.0))
			steps = append(steps, sim.Step{Kind: sim.StepSleep, Sleep: 5 * time.Millisecond})
		}
		steps = append(steps, sim.Step{Kind: sim.StepBlock})
		return sim.Script{Steps: steps}
	})

	m := testManager(t, clock)
	if err := m.Register(DeviceConfig{
		ID: "t1", Kind: sensor.KindTemperature, Dialer: dev.Dialer(), Spec: tempSpec(),
		Reconnect: fastReconnect(),
	}); err != nil {
		t.Fatal(err)
	}
	if err := m.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	// 重复启动必须被拒绝。
	if err := m.Start(context.Background()); !errors.Is(err, ErrAlreadyStarted) {
		t.Fatalf("repeat start err=%v", err)
	}

	if !m.WaitForHealth("t1", Healthy, 2*time.Second) {
		t.Fatalf("never became healthy, status=%+v", m.Status())
	}
	r, err := m.GetReading("t1")
	if err != nil {
		t.Fatalf("get reading: %v", err)
	}
	if r.Value != 25.0 || r.Verdict != sensor.VerdictOK {
		t.Fatalf("reading=%+v", r)
	}
	if len(m.Drain("t1")) < 2 {
		t.Fatal("queue should contain accepted readings")
	}
	// 未注册设备。
	if _, err := m.GetReading("nope"); !errors.Is(err, ErrDeviceNotFound) {
		t.Fatalf("want ErrDeviceNotFound, got %v", err)
	}
}

// 场景2：半包与粘包必须在驱动端被正确还原成两帧。
func TestManagerHalfAndStickyFrames(t *testing.T) {
	clock := newFakeClock()
	f1 := frame.Frame{Type: frame.TypeReading, Seq: 1, TS: clock.Now().UnixMilli(), Payload: sensor.EncodeReadingPayload(21.0)}
	f2 := frame.Frame{Type: frame.TypeReading, Seq: 2, TS: clock.Now().UnixMilli() + 10, Payload: sensor.EncodeReadingPayload(22.0)}

	dev := sim.New(func(attempt int) sim.Script {
		return sim.Script{Steps: []sim.Step{
			// f1 半包：分两次写，中间等待。
			{Kind: sim.StepSendFrame, Frame: f1, Split: true, SplitDelay: 20 * time.Millisecond},
			// f2 与下一次数据粘包；这里直接让 f2 单独粘住无意义，
			// 改为两帧同一次 write：PackWithNext。
			{Kind: sim.StepSendFrame, Frame: f2, PackWithNext: true},
			readingStep(3, clock, 20, 23.0),
			{Kind: sim.StepBlock},
		}}
	})

	m := testManager(t, clock)
	_ = m.Register(DeviceConfig{ID: "t2", Dialer: dev.Dialer(), Spec: tempSpec(), Reconnect: fastReconnect()})
	_ = m.Start(context.Background())

	ok := waitCondition(2*time.Second, func() bool {
		st := m.Status()[0]
		return st.Counters.GoodFrames == 3 && st.Counters.Accepted == 3
	})
	if !ok {
		t.Fatalf("want 3 good accepted frames across half+sticky boundary, got %+v", m.Status())
	}
}

// 场景3：坏帧不得污染后续帧；持续坏帧触发降级。
func TestManagerBadFramesAndDegrade(t *testing.T) {
	clock := newFakeClock()
	good := frame.Frame{Type: frame.TypeReading, Seq: 1, TS: clock.Now().UnixMilli(), Payload: sensor.EncodeReadingPayload(20.0)}
	goodRaw, _ := frame.Encode(&good)

	var steps []sim.Step
	// 先发足够多的好帧变 Healthy，再大量坏帧把坏帧率拉过阈值。
	for i := 0; i < 25; i++ {
		f := good
		f.Seq = uint16(i + 1)
		f.TS = clock.Now().UnixMilli() + int64(i)
		steps = append(steps, sim.Step{Kind: sim.StepSendFrame, Frame: f})
	}
	for i := 0; i < 25; i++ {
		corrupt := append([]byte(nil), goodRaw...)
		corrupt[15] ^= byte(i + 1) // 翻转 payload 首字节，CRC 必然不匹配
		steps = append(steps, rawStep(corrupt))
	}
	// 末尾长 Sleep 保持连接静默但不断开；本用例通过放宽 stall 参数
	// 排除“阻塞隔离”路径，专注验证坏帧率驱动的降级。
	steps = append(steps, sim.Step{Kind: sim.StepSleep, Sleep: 5 * time.Second})

	dev := sim.New(func(attempt int) sim.Script { return sim.Script{Steps: steps} })
	m := testManager(t, clock)
	traj := newHealthTracer()
	_ = m.Register(DeviceConfig{
		ID: "t3", Dialer: dev.Dialer(), Spec: tempSpec(),
		BadWindow: 20, MaxBadRate: 0.5, Reconnect: fastReconnect(),
		IdleTimeout: 2 * time.Second, MaxStalls: 100,
		OnHealth: traj.callback,
	})
	_ = m.Start(context.Background())

	// 25 好帧送达极快，Healthy 可能只存在约 1ms，轮询必然不可靠。
	// 直接订阅内部健康迁移轨迹来精确断言。
	if !waitCondition(2*time.Second, func() bool {
		return traj.current() == Degraded && m.Status()[0].Counters.BadFrames >= 10
	}) {
		t.Fatalf("should pass through healthy then degrade, trajectory=%v status=%+v",
			traj.transitions(), m.Status())
	}
	if !traj.sawHealthy() {
		t.Fatalf("device never reached healthy: trajectory=%v", traj.transitions())
	}
}

// 场景4：越界与乱序时间戳读数被标记拒绝，且不污染后续。
func TestManagerOutOfRangeAndOutOfOrder(t *testing.T) {
	clock := newFakeClock()
	dev := sim.New(func(attempt int) sim.Script {
		return sim.Script{Steps: []sim.Step{
			readingStep(1, clock, 0, 25.0),
			readingStep(2, clock, 100, 125.0), // 越界
			readingStep(3, clock, 200, 50.0),  // 合法
			readingStep(4, clock, 50, 51.0),   // 设备时间戳回退 -> 乱序
			readingStep(5, clock, 300, 52.0),  // 恢复正常
			{Kind: sim.StepBlock},
		}}
	})
	m := testManager(t, clock)
	_ = m.Register(DeviceConfig{ID: "t4", Dialer: dev.Dialer(), Spec: tempSpec(), Reconnect: fastReconnect()})
	_ = m.Start(context.Background())

	ok := waitCondition(2*time.Second, func() bool {
		c := m.Status()[0].Counters
		return c.Accepted == 3 && c.RejectedOOR == 1 && c.RejectedOOO == 1
	})
	if !ok {
		t.Fatalf("want 3 accepted / 1 oor / 1 ooo, got %+v", m.Status())
	}
	// 最新读数应是 seq5（seq4 被拒后基线未回退）。
	r, err := m.GetReading("t4")
	if err != nil {
		t.Fatal(err)
	}
	if r.Seq != 5 || r.Value != 52.0 {
		t.Fatalf("latest reading must be seq 5, got seq=%d v=%v", r.Seq, r.Value)
	}
}

// 场景5：单设备阻塞触发空闲超时计数并隔离，且不影响另一设备。
func TestManagerBlockedDeviceIsolationDoesNotAffectOthers(t *testing.T) {
	clock := newFakeClock()
	var seq uint16
	blocked := sim.New(func(attempt int) sim.Script {
		// 每次重连都静默阻塞。
		return sim.Script{Steps: []sim.Step{{Kind: sim.StepBlock}}}
	})
	healthy := sim.New(func(attempt int) sim.Script {
		var steps []sim.Step
		for i := 0; i < 3; i++ {
			seq++
			steps = append(steps, readingStep(seq, clock, int64(i), 18.0))
		}
		steps = append(steps, sim.Step{Kind: sim.StepBlock})
		return sim.Script{Steps: steps}
	})

	m := testManager(t, clock)
	_ = m.Register(DeviceConfig{
		ID: "blocked", Dialer: blocked.Dialer(), Spec: tempSpec(),
		IdleTimeout: 50 * time.Millisecond, MaxStalls: 2, Reconnect: fastReconnect(),
	})
	_ = m.Register(DeviceConfig{ID: "fine", Dialer: healthy.Dialer(), Spec: tempSpec(), Reconnect: fastReconnect()})
	_ = m.Start(context.Background())

	if !waitCondition(3*time.Second, func() bool {
		st := map[string]DeviceStatus{}
		for _, s := range m.Status() {
			st[s.ID] = s
		}
		return st["blocked"].Health == Isolated && st["blocked"].Counters.IdleTimeouts >= 2 &&
			st["fine"].Health == Healthy && st["fine"].Counters.Accepted == 3
	}) {
		t.Fatalf("blocked device must isolate while the other stays healthy: %+v", m.Status())
	}

	// 隔离设备不得对外提供读数。
	if _, err := m.GetReading("blocked"); !errors.Is(err, ErrDeviceUnavailable) {
		t.Fatalf("isolated device err=%v want ErrDeviceUnavailable", err)
	}
	r, err := m.GetReading("fine")
	if err != nil || r.Value != 18.0 {
		t.Fatalf("healthy peer must still serve readings: %v %+v", err, r)
	}
}

// 场景6：故障断开后重连成功，必须走观察期且不沿用故障前状态（队列被清空）。
func TestManagerRecoveryAfterDisconnect(t *testing.T) {
	clock := newFakeClock()
	var seqMu sync.Mutex
	dev := sim.New(func(attempt int) sim.Script {
		seqMu.Lock()
		defer seqMu.Unlock()
		var steps []sim.Step
		for i := 0; i < 3; i++ {
			steps = append(steps, readingStep(uint16(100*attempt+i), clock, int64(i), 30.0+float64(attempt)))
		}
		if attempt == 1 {
			// 首次连接主动断开，模拟链路故障。
			steps = append(steps, sim.Step{Kind: sim.StepClose})
		} else {
			steps = append(steps, sim.Step{Kind: sim.StepBlock})
		}
		return sim.Script{Steps: steps}
	})

	m := testManager(t, clock)
	_ = m.Register(DeviceConfig{ID: "rec", Dialer: dev.Dialer(), Spec: tempSpec(), Reconnect: fastReconnect()})
	_ = m.Start(context.Background())

	// 等首次连接的 3 帧落地。
	if !waitCondition(2*time.Second, func() bool { return m.Status()[0].Counters.Accepted >= 3 }) {
		t.Fatal("initial frames not accepted")
	}
	before := m.Drain("rec")
	if len(before) < 3 {
		t.Fatalf("initial drain=%d", len(before))
	}

	// 断开后应隔离并自动重连，重连后再次进入 Degraded 观察期。
	if !waitCondition(3*time.Second, func() bool {
		return dev.Attempts() >= 2
	}) {
		t.Fatalf("device did not reconnect, status=%+v attempts=%d", m.Status(), dev.Attempts())
	}

	// 重连成功后应恢复 Healthy。
	if !m.WaitForHealth("rec", Healthy, 3*time.Second) {
		t.Fatalf("not healthy after recovery: %+v", m.Status())
	}

	// 恢复后最新读数必须来自第二次连接（seq>=200），证明故障前的
	// 读数缓存与时间戳基线均未被沿用。
	r, err := m.GetReading("rec")
	if err != nil {
		t.Fatalf("post-recovery get reading: %v", err)
	}
	if r.Seq < 200 {
		t.Fatalf("stale pre-failure reading survived reset: seq=%d", r.Seq)
	}
	// 队列中此刻能取到的读数也全部不得早于故障（避免断言与调度竞态，
	// 只校验“不存在旧数据”，而非队列长度）。
	for _, rd := range m.Drain("rec") {
		if rd.Seq < 200 {
			t.Fatalf("stale reading in queue after recovery: seq=%d", rd.Seq)
		}
	}
}

// 场景7：注入时钟推进后，设备时间戳滞后的读数被标记 stale 并拒绝。
func TestManagerStaleTimestampRejected(t *testing.T) {
	clock := newFakeClock()
	dev := sim.New(func(attempt int) sim.Script {
		base := clock.Now().UnixMilli()
		return sim.Script{Steps: []sim.Step{
			// 第 1 条：新鲜，接受。
			readingStep(1, clock, 0, 22.0),
			// 第 2 条：设备时间戳落后 5s，超过 MaxAge(2s)，必须拒绝。
			{Kind: sim.StepSendFrame, Frame: frame.Frame{
				Type: frame.TypeReading, Seq: 2, TS: base - 5000,
				Payload: sensor.EncodeReadingPayload(23.0),
			}},
			// 第 3 条：恢复新鲜且时间戳正常推进，接受。
			readingStep(3, clock, 100, 24.0),
			{Kind: sim.StepBlock},
		}}
	})

	m := testManager(t, clock)
	_ = m.Register(DeviceConfig{ID: "stale", Dialer: dev.Dialer(), Spec: tempSpec(), Reconnect: fastReconnect()})
	_ = m.Start(context.Background())

	ok := waitCondition(2*time.Second, func() bool {
		c := m.Status()[0].Counters
		return c.Accepted == 2 && c.RejectedStale == 1
	})
	if !ok {
		t.Fatalf("want 2 accepted / 1 stale, got %+v", m.Status())
	}
	r, err := m.GetReading("stale")
	if err != nil {
		t.Fatal(err)
	}
	if r.Seq != 3 {
		t.Fatalf("latest usable reading must be seq 3, got seq=%d", r.Seq)
	}
}
