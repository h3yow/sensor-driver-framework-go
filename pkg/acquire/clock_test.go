package acquire

import (
	"testing"
	"time"

	"github.com/h3yow/sensor-driver-framework-go/pkg/clockalign"
	"github.com/h3yow/sensor-driver-framework-go/pkg/frame"
	"github.com/h3yow/sensor-driver-framework-go/pkg/simdev"
)

// 场景：设备时钟明显超前（稳定偏快 10s）。
// 应自动估计偏差并把读数修正到统一时间轴，标记 Adjusted，RawTS 可查。
func TestClockAheadDeviceAligned(t *testing.T) {
	dev, err := simdev.New("ahead", simdev.Behavior{IntervalMs: 5, TsOffsetMs: 10000}, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer dev.Close()

	m := NewManagerWithConfig(Config{Clock: clockalign.Config{Enabled: true, MinSamples: 3}})
	m.Add("ahead", dev.Addr)
	rs := runManager(m, 1200*time.Millisecond)

	good, adjusted, unaligned := 0, 0, 0
	for _, r := range rs {
		switch r.Quality {
		case QualityGood:
			good++
			if r.Adjusted {
				adjusted++
			}
			if age := time.Since(r.TS); age > 2*time.Second || age < -2*time.Second {
				t.Fatalf("aligned ts %s too far from now (%v)", r.TS.Format(time.RFC3339Nano), age)
			}
			if r.RawTS.Before(time.Now().Add(8*time.Second)) {
				t.Fatalf("raw ts %s should be ~10s ahead", r.RawTS.Format(time.RFC3339Nano))
			}
		case QualityUnaligned:
			unaligned++
		default:
			t.Fatalf("unexpected quality %v", r.Quality)
		}
	}
	t.Logf("good=%d adjusted=%d unaligned(learning)=%d", good, adjusted, unaligned)
	if good < 50 {
		t.Fatalf("only %d good readings", good)
	}
	if adjusted != good {
		t.Fatalf("all good readings should be adjusted, got %d/%d", adjusted, good)
	}
	off, ok := m.ClockOffsetOf("ahead")
	if !ok {
		t.Fatal("offset should be available after alignment")
	}
	if off > -8*time.Second || off < -12*time.Second {
		t.Fatalf("estimated offset %v, want ~-10s", off)
	}
	t.Logf("estimated offset=%v", off)
}

// 场景：单条异常时间戳后紧跟正常读数。
// 异常读数被标记且不得成为基线；紧跟的正常读数不得误判为乱序。
func TestClockAnomalyThenNormalNotOutOfOrder(t *testing.T) {
	m := NewManagerWithConfig(Config{Clock: clockalign.Config{Enabled: true, MinSamples: 3, MaxFutureMs: 2000}})
	m.Add("d", "unused")
	d := m.devs["d"]

	mk := func(ts time.Time) frame.Frame {
		return frame.Frame{Type: 1, Payload: simdev.EncodePayload(ts, 50)}
	}
	// 建立对齐（学习窗口内判 Unaligned）
	for i := 0; i < 3; i++ {
		m.judge(d, mk(time.Now()))
	}
	if r, ok := m.judge(d, mk(time.Now())); !ok || r.Quality != QualityGood {
		t.Fatalf("aligned reading judged %v", r.Quality)
	}
	// 单条异常：超前 10s
	r, ok := m.judge(d, mk(time.Now().Add(10*time.Second)))
	if ok || r.Quality != QualityClockAhead {
		t.Fatalf("anomalous reading judged %v, want ClockAhead", r.Quality)
	}
	t.Logf("anomaly judged %s, raw=%s", r.Quality, r.RawTS.Format(time.RFC3339Nano))
	// 紧跟的正常读数：必须 Good，不得被异常读数带偏或误判乱序
	r, ok = m.judge(d, mk(time.Now()))
	if !ok || r.Quality != QualityGood {
		t.Fatalf("normal reading after anomaly judged %v, want Good", r.Quality)
	}
	t.Logf("normal reading after anomaly judged %s", r.Quality)
}

// 场景：运行中时间跳变（设备时钟突然回拨 1 小时）。
// 应识别滞后异常、触发重建，恢复后继续产出对齐后的 Good 读数。
func TestClockJumpRecovery(t *testing.T) {
	dev, err := simdev.New("jumper", simdev.Behavior{IntervalMs: 5, TsJumpAfter: 20, TsJumpMs: -3600000}, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer dev.Close()

	m := NewManagerWithConfig(Config{Clock: clockalign.Config{Enabled: true, MinSamples: 3, ResetThreshold: 3}})
	m.Add("jumper", dev.Addr)
	rs := runManager(m, 1500*time.Millisecond)

	sawBehind := false
	goodAfter := 0
	for _, r := range rs {
		if r.Quality == QualityClockBehind {
			sawBehind = true
		}
		if sawBehind && r.Quality == QualityGood {
			goodAfter++
		}
	}
	t.Logf("sawBehind=%v goodAfterJump=%d total=%d", sawBehind, goodAfter, len(rs))
	if !sawBehind {
		t.Fatal("expected ClockBehind readings after clock jump")
	}
	if goodAfter < 10 {
		t.Fatalf("only %d good readings after jump recovery", goodAfter)
	}
	off, ok := m.ClockOffsetOf("jumper")
	if !ok || off < 59*time.Minute || off > 61*time.Minute {
		t.Fatalf("offset %v (ok=%v), want ~1h after rebuild", off, ok)
	}
	t.Logf("rebuilt offset=%v", off)
}

// 场景：设备重启后从零重新计时（1970 纪元）。
// 应识别 Invalid、触发纪元重置并重新对齐，健康度保持/回升到 Good。
func TestEpochRestartRecovery(t *testing.T) {
	dev, err := simdev.New("reboot", simdev.Behavior{IntervalMs: 5, TsFromZero: true}, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer dev.Close()

	m := NewManagerWithConfig(Config{
		DegradeThreshold: 3, IsolateThreshold: 6, RecoveryRequired: 3,
		Clock: clockalign.Config{Enabled: true, MinSamples: 3, ResetThreshold: 3},
	})
	m.Add("reboot", dev.Addr)
	rs := runManager(m, 1500*time.Millisecond)

	sawInvalid := false
	good := 0
	var lastGood Reading
	for _, r := range rs {
		if r.Quality == QualityClockInvalid {
			sawInvalid = true
		}
		if r.Quality == QualityGood {
			good++
			lastGood = r
		}
	}
	t.Logf("sawInvalid=%v good=%d total=%d", sawInvalid, good, len(rs))
	if !sawInvalid {
		t.Fatal("expected ClockInvalid readings from zero-based clock")
	}
	if good < 10 {
		t.Fatalf("only %d good readings after epoch restart recovery", good)
	}
	if !lastGood.Adjusted {
		t.Fatal("recovered readings should be adjusted")
	}
	if age := time.Since(lastGood.TS); age > 2*time.Second {
		t.Fatalf("aligned ts age %v after epoch restart", age)
	}
	if h := m.HealthOf("reboot"); h != HealthGood {
		t.Fatalf("health=%s, want Good (must not stick in degraded/isolated)", h)
	}
	t.Logf("health=%s, last aligned ts=%s", m.HealthOf("reboot"), lastGood.TS.Format(time.RFC3339Nano))
}

// 场景：设备长时间无法对齐（持续完全不合理的时间戳）。
// 应按策略降级，且其数据不得以 Good 进入结果。
func TestUnalignedDeviceDegraded(t *testing.T) {
	dev, err := simdev.New("broken", simdev.Behavior{IntervalMs: 5, TsFromZero: true}, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer dev.Close()

	m := NewManagerWithConfig(Config{
		DegradeThreshold: 2, IsolateThreshold: 4,
		Clock: clockalign.Config{Enabled: true, ResetThreshold: 1000, AlignTimeoutMs: 300},
	})
	m.Add("broken", dev.Addr)
	rs := runManager(m, 1500*time.Millisecond)

	good := 0
	for _, r := range rs {
		if r.Quality == QualityGood {
			good++
		}
	}
	h := m.HealthOf("broken")
	t.Logf("health=%s good=%d total=%d", h, good, len(rs))
	if good != 0 {
		t.Fatalf("unaligned device leaked %d good readings into results", good)
	}
	if h == HealthGood {
		t.Fatal("persistently unaligned device should be degraded")
	}
}

// 场景：一台设备时钟异常不影响其它设备采集。
func TestClockFaultDeviceDoesNotAffectOthers(t *testing.T) {
	broken, err := simdev.New("broken", simdev.Behavior{IntervalMs: 5, TsFromZero: true}, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer broken.Close()
	good, err := simdev.New("good", simdev.Behavior{IntervalMs: 5}, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer good.Close()

	m := NewManagerWithConfig(Config{
		Clock: clockalign.Config{Enabled: true, ResetThreshold: 1000, AlignTimeoutMs: 300},
	})
	m.Add("broken", broken.Addr)
	m.Add("good", good.Addr)
	rs := runManager(m, 1000*time.Millisecond)

	goodCount := 0
	for _, r := range rs {
		if r.Device == "good" && r.Quality == QualityGood {
			goodCount++
		}
	}
	t.Logf("good device good readings: %d", goodCount)
	if goodCount < 50 {
		t.Fatalf("good device produced only %d good readings; broken device affected it", goodCount)
	}
}
