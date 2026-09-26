package acquire

import (
	"testing"
	"time"

	"github.com/h3yow/sensor-driver-framework-go/pkg/frame"
	"github.com/h3yow/sensor-driver-framework-go/pkg/simdev"
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
	if r, ok := m.judge(d, mk(time.Now(), 50)); !ok || r.Quality != QualityGood {
		t.Fatalf("good reading judged %v", r.Quality)
	}
	// 越界
	if r, ok := m.judge(d, mk(time.Now(), 500)); ok || r.Quality != QualityOutOfRange {
		t.Fatalf("out-of-range judged %v", r.Quality)
	}
	// 乱序（基线已建立）
	if r, ok := m.judge(d, mk(time.Now().Add(-time.Minute), 50)); ok || r.Quality != QualityOutOfOrder {
		t.Fatalf("out-of-order judged %v", r.Quality)
	}
	// 过期（时间戳在未来方向推进，但早于 now-staleAfter）
	d.hasBase = false
	if r, ok := m.judge(d, mk(time.Now().Add(-5*time.Second), 50)); ok || r.Quality != QualityStale {
		t.Fatalf("stale judged %v", r.Quality)
	}
}
