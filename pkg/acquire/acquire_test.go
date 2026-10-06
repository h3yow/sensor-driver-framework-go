package acquire

import (
	"testing"
	"time"

	"github.com/h3yow/sensor-driver-framework-go/pkg/frame"
	"github.com/h3yow/sensor-driver-framework-go/pkg/simdev"
	"github.com/h3yow/sensor-driver-framework-go/pkg/timesync"
)

// runManager 启动采集并在 dur 后停止，返回收集到的读数。
func runManager(m *Manager, dur time.Duration) []Reading {
	stop := make(chan struct{})
	out := make(chan Reading, 4096)
	done := make(chan struct{})
	go func() { m.Run(stop, out); close(done) }()
	time.Sleep(dur)
	close(stop)
	<-done
	close(out)
	var rs []Reading
	for r := range out {
		rs = append(rs, r)
	}
	return rs
}

func TestConcurrentBlockingDeviceDoesNotSlowOthers(t *testing.T) {
	good, err := simdev.New("good", simdev.Behavior{IntervalMs: 5}, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer good.Close()
	// 阻塞设备：每 2 帧阻塞 5 秒
	blocker, err := simdev.New("blocker", simdev.Behavior{BlockAfter: 2, BlockMs: 5000}, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Close()

	m := NewManager()
	m.Add("good", good.Addr)
	m.Add("blocker", blocker.Addr)

	rs := runManager(m, 800*time.Millisecond)
	goodCount := 0
	for _, r := range rs {
		if r.Device == "good" {
			goodCount++
		}
	}
	// 800ms / 5ms 间隔，扣除启动开销，good 设备应产出大量读数
	if goodCount < 50 {
		t.Fatalf("good device produced only %d readings in 800ms; blocker slowed it down", goodCount)
	}
	t.Logf("good device readings: %d", goodCount)
}

func TestOutOfOrderMarked(t *testing.T) {
	dev, err := simdev.New("reorder", simdev.Behavior{ReorderWindow: 2}, 8)
	if err != nil {
		t.Fatal(err)
	}
	defer dev.Close()

	m := NewManager()
	m.Add("reorder", dev.Addr)
	rs := runManager(m, 1500*time.Millisecond)

	ooo := 0
	for _, r := range rs {
		if r.Quality == QualityOutOfOrder {
			ooo++
		}
	}
	if ooo == 0 {
		t.Fatal("expected out-of-order readings to be marked")
	}
	t.Logf("out-of-order readings: %d/%d", ooo, len(rs))
}

func TestStaleMarked(t *testing.T) {
	dev, err := simdev.New("stale", simdev.Behavior{TsOffsetMs: -5000}, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer dev.Close()

	m := NewManager()
	m.Add("stale", dev.Addr)
	rs := runManager(m, 1500*time.Millisecond)

	stale := 0
	for _, r := range rs {
		if r.Quality == QualityStale {
			stale++
		}
	}
	if stale == 0 {
		t.Fatal("expected stale readings to be marked")
	}
}

func TestBadFramesTriggerIsolation(t *testing.T) {
	// 全部帧均为坏帧：连续异常应触发恢复动作并最终隔离
	dev, err := simdev.New("allbad", simdev.Behavior{CorruptEvery: 1}, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer dev.Close()

	m := NewManagerWithConfig(Config{DegradeThreshold: 2, IsolateThreshold: 4, ReadTimeoutMs: 100})
	m.Add("allbad", dev.Addr)
	stop := make(chan struct{})
	out := make(chan Reading, 1024)
	done := make(chan struct{})
	go func() { m.Run(stop, out); close(done) }()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if m.HealthOf("allbad") == HealthIsolated {
			close(stop)
			<-done
			return // 成功隔离
		}
		time.Sleep(50 * time.Millisecond)
	}
	close(stop)
	<-done
	t.Fatalf("device with all bad frames was not isolated, health=%s", m.HealthOf("allbad"))
}

func TestHealthTransitionsAndRecoveryReset(t *testing.T) {
	m := NewManagerWithConfig(Config{DegradeThreshold: 2, IsolateThreshold: 4, RecoveryRequired: 3})
	m.Add("d", "unused")
	d := m.devs["d"]

	// 建立基线
	d.lastTS = time.Now()
	d.hasBase = true

	m.onFault(d, "f1")
	if h := m.HealthOf("d"); h != HealthGood {
		t.Fatalf("after 1 fault health=%s, want Good", h)
	}
	m.onFault(d, "f2")
	if h := m.HealthOf("d"); h != HealthDegraded {
		t.Fatalf("after 2 faults health=%s, want Degraded", h)
	}
	m.onFault(d, "f3")
	m.onFault(d, "f4")
	if h := m.HealthOf("d"); h != HealthIsolated {
		t.Fatalf("after 4 faults health=%s, want Isolated", h)
	}
	// 1~2 条良好读数不足以恢复
	m.onGood(d)
	m.onGood(d)
	if h := m.HealthOf("d"); h != HealthIsolated {
		t.Fatalf("after 2 good readings health=%s, want Isolated", h)
	}
	// 第 3 条恢复，且基线被重置
	m.onGood(d)
	if h := m.HealthOf("d"); h != HealthGood {
		t.Fatalf("after 3 good readings health=%s, want Good", h)
	}
	if d.hasBase {
		t.Fatal("baseline should be reset after recovery")
	}
	// 恢复后故障计数清零：再次故障需重新累计
	m.onFault(d, "g1")
	if h := m.HealthOf("d"); h != HealthGood {
		t.Fatalf("after recovery 1 fault health=%s, want Good", h)
	}
}

func TestJudgeQuality(t *testing.T) {
	m := NewManagerWithConfig(Config{MinValue: 0, MaxValue: 100, StaleAfterMs: 1000})
	m.Add("d", "unused")
	d := m.devs["d"]

	mk := func(ts time.Time, v float64) frame.Frame {
		return frame.Frame{Type: 1, Payload: simdev.EncodePayload(ts, v)}
	}
	// 良好读数
	if r := m.judge(d, mk(time.Now(), 50)); r.Quality != QualityGood {
		t.Fatalf("good reading judged %v", r.Quality)
	}
	// 越界
	if r := m.judge(d, mk(time.Now(), 500)); r.Quality != QualityOutOfRange {
		t.Fatalf("out-of-range judged %v", r.Quality)
	}
	// 乱序（基线已建立）
	if r := m.judge(d, mk(time.Now().Add(-time.Minute), 50)); r.Quality != QualityOutOfOrder {
		t.Fatalf("out-of-order judged %v", r.Quality)
	}
	// 过期（时间戳在未来方向推进，但早于 now-staleAfter）
	d.hasBase = false
	if r := m.judge(d, mk(time.Now().Add(-5*time.Second), 50)); r.Quality != QualityStale {
		t.Fatalf("stale judged %v", r.Quality)
	}
}

// ---------- 时间对齐与时钟异常防护 ----------

// tsTestConfig 返回启用时间对齐的测试配置（小超时便于快速判定）。
func tsTestConfig() Config {
	return Config{
		ReadTimeoutMs: 100,
		TimeSync: &timesync.Config{
			MinSamples:     3,
			AcceptWindowMs: 1000,
			AheadMs:        30000,
			BehindMs:       30000,
			JumpConfirm:    3,
			AlignTimeoutMs: 500,
		},
	}
}

// runManagerUntil 启动采集，直到 cond 满足或超时，返回收集到的读数。
func runManagerUntil(m *Manager, timeout time.Duration, cond func() bool) []Reading {
	stop := make(chan struct{})
	out := make(chan Reading, 8192)
	done := make(chan struct{})
	go func() { m.Run(stop, out); close(done) }()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond != nil && cond() {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	close(stop)
	<-done
	close(out)
	var rs []Reading
	for r := range out {
		rs = append(rs, r)
	}
	return rs
}

func ofDevice(rs []Reading, name string) []Reading {
	var out []Reading
	for _, r := range rs {
		if r.Device == name {
			out = append(out, r)
		}
	}
	return out
}

// 场景：设备时钟明显超前（尖峰帧比基准快 2 分钟），应被明确标记为 ClockAhead。
func TestClockAheadMarked(t *testing.T) {
	dev, err := simdev.New("ahead", simdev.Behavior{IntervalMs: 10, TsSpikeEvery: 5, TsSpikeMs: 120000}, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer dev.Close()

	m := NewManagerWithConfig(tsTestConfig())
	m.Add("ahead", dev.Addr)
	rs := runManagerUntil(m, 2*time.Second, nil)

	ahead, adjusted := 0, 0
	for _, r := range rs {
		if r.Quality == QualityClockAhead {
			ahead++
		}
		if r.Adjusted && r.Quality == QualityGood {
			adjusted++
		}
	}
	if ahead == 0 {
		t.Fatal("expected clock-ahead readings to be marked")
	}
	if adjusted == 0 {
		t.Fatal("expected normal readings to be aligned and adjusted")
	}
	t.Logf("clock-ahead=%d adjusted-good=%d total=%d", ahead, adjusted, len(rs))
}

// 场景：单条异常时间戳后紧跟正常读数——异常不得成为比较基准，
// 紧跟的正常读数不得被误判为乱序或异常。
func TestSingleAnomalyThenNormalNotMisjudged(t *testing.T) {
	dev, err := simdev.New("spike", simdev.Behavior{IntervalMs: 10, TsSpikeEvery: 5, TsSpikeMs: 120000}, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer dev.Close()

	m := NewManagerWithConfig(tsTestConfig())
	m.Add("spike", dev.Addr)
	rs := ofDevice(runManagerUntil(m, 2*time.Second, nil), "spike")

	spikes, verified := 0, 0
	for i, r := range rs {
		if r.Quality != QualityClockAhead {
			continue
		}
		spikes++
		if i+1 >= len(rs) {
			continue // 采集恰好停在尖峰帧，下一条尚未到达
		}
		next := rs[i+1]
		if next.Quality != QualityGood {
			t.Fatalf("reading after anomaly judged %s, want Good (anomaly poisoned the baseline)", next.Quality)
		}
		verified++
	}
	if spikes < 2 || verified < 2 {
		t.Fatalf("expected at least 2 anomalies with verified followers, got spikes=%d verified=%d", spikes, verified)
	}
	t.Logf("anomalies=%d verified-followers=%d, every following reading judged Good", spikes, verified)
}

// 场景：运行中设备时钟跳变（+5 分钟），应检测跳变、重建时间序并恢复对齐。
func TestClockJumpRealigns(t *testing.T) {
	dev, err := simdev.New("jump", simdev.Behavior{IntervalMs: 10, TsJumpAfter: 15, TsJumpMs: 300000}, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer dev.Close()

	m := NewManagerWithConfig(tsTestConfig())
	m.Add("jump", dev.Addr)
	rs := ofDevice(runManagerUntil(m, 2*time.Second, nil), "jump")

	ahead := 0
	for _, r := range rs {
		if r.Quality == QualityClockAhead {
			ahead++
		}
	}
	if ahead == 0 {
		t.Fatal("expected clock-ahead anomalies at the jump")
	}
	// 跳变后应重新对齐：尾部读数全部 Good 且 Adjusted，统一时间接近当前时间
	if len(rs) < 10 {
		t.Fatalf("too few readings: %d", len(rs))
	}
	for _, r := range rs[len(rs)-10:] {
		if r.Quality != QualityGood || !r.Adjusted {
			t.Fatalf("post-jump reading not realigned: quality=%s adjusted=%v", r.Quality, r.Adjusted)
		}
		if d := time.Since(r.AlignedTS); d < -2*time.Second || d > 2*time.Second {
			t.Fatalf("post-jump aligned ts off by %v", d)
		}
	}
	if h := m.HealthOf("jump"); h != HealthGood {
		t.Fatalf("health=%s after jump recovery, want Good", h)
	}
	t.Logf("jump anomalies=%d, realigned and health=Good", ahead)
}

// 场景：设备重启（断开重连 + 时钟从零重计），应重新建立时间序并恢复健康。
func TestRebootReconnectRecovers(t *testing.T) {
	dev, err := simdev.New("reboot", simdev.Behavior{IntervalMs: 10, RebootAfter: 15}, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer dev.Close()

	m := NewManagerWithConfig(tsTestConfig())
	m.Add("reboot", dev.Addr)
	rs := ofDevice(runManagerUntil(m, 3*time.Second, nil), "reboot")

	// 重启后设备上报 uptime（1970 起算）：应存在原始时间很小但对齐到当前的读数
	realigned := 0
	for _, r := range rs {
		if r.TS.Year() < 2000 && r.Adjusted && r.Quality == QualityGood {
			if d := time.Since(r.AlignedTS); d >= -2*time.Second && d <= 2*time.Second {
				realigned++
			}
		}
		if r.Quality == QualityOutOfOrder {
			t.Fatal("post-reboot reading misjudged as OutOfOrder (stale baseline)")
		}
	}
	if realigned == 0 {
		t.Fatal("expected uptime readings to be realigned after reboot")
	}
	if h := m.HealthOf("reboot"); h != HealthGood {
		t.Fatalf("health=%s after reboot recovery, want Good", h)
	}
	t.Logf("post-reboot realigned readings=%d, health=Good", realigned)
}

// 场景：设备时间戳互相矛盾、长时间无法对齐——按既定策略降级并限制其数据，
// 且不影响其它设备继续采集。
func TestUnalignableDeviceLimited(t *testing.T) {
	bad, err := simdev.New("unalignable", simdev.Behavior{IntervalMs: 10, TsSpikeEvery: 2, TsSpikeMs: 20000}, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer bad.Close()
	good, err := simdev.New("good2", simdev.Behavior{IntervalMs: 10}, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer good.Close()

	m := NewManagerWithConfig(tsTestConfig())
	m.Add("unalignable", bad.Addr)
	m.Add("good2", good.Addr)

	rs := runManagerUntil(m, 5*time.Second, func() bool {
		return m.HealthOf("unalignable") != HealthGood
	})
	if h := m.HealthOf("unalignable"); h == HealthGood {
		t.Fatal("unalignable device was not degraded")
	}
	limited := 0
	for _, r := range ofDevice(rs, "unalignable") {
		if r.TimeState == timesync.StateDegraded {
			limited++
		}
	}
	if limited == 0 {
		t.Fatal("expected readings marked TimeState=Degraded (limited)")
	}
	goodCount := 0
	for _, r := range ofDevice(rs, "good2") {
		if r.Quality == QualityGood {
			goodCount++
		}
	}
	if goodCount < 20 {
		t.Fatalf("good device affected: only %d good readings", goodCount)
	}
	t.Logf("unalignable health=%s limited=%d, good device unaffected (%d good)",
		m.HealthOf("unalignable"), limited, goodCount)
}

// 场景：多台时钟偏移不同的设备对齐到统一时间轴，可跨设备排序比较。
func TestCrossDeviceUnifiedTimeline(t *testing.T) {
	fast, err := simdev.New("fast", simdev.Behavior{IntervalMs: 10, TsOffsetMs: 2000}, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer fast.Close()
	slow, err := simdev.New("slow", simdev.Behavior{IntervalMs: 10, TsOffsetMs: -3000}, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer slow.Close()

	m := NewManagerWithConfig(tsTestConfig())
	m.Add("fast", fast.Addr)
	m.Add("slow", slow.Addr)
	rs := runManagerUntil(m, 1500*time.Millisecond, nil)

	// 设备时钟偏移不同（+2s / -3s），对齐后应落在同一时间轴：
	// AlignedTS ≈ TS - 设备偏移，与采集时刻无关（读数可在通道中缓冲）。
	expected := map[string]time.Duration{"fast": 2 * time.Second, "slow": -3 * time.Second}
	for _, name := range []string{"fast", "slow"} {
		adj := 0
		for _, r := range ofDevice(rs, name) {
			if !r.Adjusted || r.Quality != QualityGood {
				continue
			}
			adj++
			err := r.AlignedTS.Sub(r.TS.Add(-expected[name]))
			if err < -500*time.Millisecond || err > 500*time.Millisecond {
				t.Fatalf("dev=%s alignment error %v (aligned=%s raw=%s)", name, err, r.AlignedTS, r.TS)
			}
		}
		if adj == 0 {
			t.Fatalf("dev=%s produced no adjusted readings", name)
		}
		t.Logf("dev=%s adjusted readings=%d on unified timeline", name, adj)
	}
	// 确认两台设备原始时间戳确实相差约 5s（对齐前不可直接比较）
	var fLast, sLast *Reading
	for i := range rs {
		if rs[i].Device == "fast" && rs[i].Adjusted {
			fLast = &rs[i]
		}
		if rs[i].Device == "slow" && rs[i].Adjusted {
			sLast = &rs[i]
		}
	}
	if fLast == nil || sLast == nil {
		t.Fatal("missing adjusted readings")
	}
	rawDiff := fLast.TS.Sub(sLast.TS)
	alignedDiff := fLast.AlignedTS.Sub(sLast.AlignedTS)
	if rawDiff < 4*time.Second || rawDiff > 6*time.Second {
		t.Fatalf("raw ts diff=%v, want ~5s (offsets differ)", rawDiff)
	}
	if alignedDiff < -time.Second || alignedDiff > time.Second {
		t.Fatalf("aligned ts diff=%v, want ~0 (unified timeline)", alignedDiff)
	}
	t.Logf("raw diff=%v aligned diff=%v", rawDiff, alignedDiff)
}
