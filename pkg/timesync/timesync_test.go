package timesync

import (
	"testing"
	"time"
)

var base = time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)

// feed 以设备时钟 offset（设备时间 = 基准时间 - offset）逐条观测，返回最后一次结果。
func feed(a *Aligner, start time.Time, offset time.Duration, n int, step time.Duration) Result {
	var r Result
	for i := 0; i < n; i++ {
		arrival := start.Add(time.Duration(i) * step)
		dev := arrival.Add(-offset)
		r = a.Observe(dev, arrival)
	}
	return r
}

func TestAlignsAndAdjusts(t *testing.T) {
	a := New(Config{})
	offset := 500 * time.Millisecond // 设备时钟偏慢 500ms
	r := feed(a, base, offset, 3, 10*time.Millisecond)
	if r.State != StateAligned {
		t.Fatalf("state=%s, want Aligned", r.State)
	}
	// 对齐后：统一时间应还原为基准时间
	arrival := base.Add(100 * time.Millisecond)
	res := a.Observe(arrival.Add(-offset), arrival)
	if !res.Adjusted {
		t.Fatal("expected Adjusted=true after alignment")
	}
	if d := res.AlignedTS.Sub(arrival); d < -5*time.Millisecond || d > 5*time.Millisecond {
		t.Fatalf("aligned ts off by %v", d)
	}
	if got := a.Offset(); got < 400*time.Millisecond || got > 600*time.Millisecond {
		t.Fatalf("offset estimate %v, want ~500ms", got)
	}
	t.Logf("aligned: offset=%v adjusted=%v", a.Offset(), res.Adjusted)
}

func TestFollowsSlowDrift(t *testing.T) {
	a := New(Config{})
	feed(a, base, 0, 3, 10*time.Millisecond)
	// 设备时钟缓慢漂移：每步多偏 1ms（远低于接受窗口）
	var r Result
	for i := 0; i < 50; i++ {
		arrival := base.Add(time.Duration(i) * 10 * time.Millisecond)
		offset := time.Duration(i) * time.Millisecond
		r = a.Observe(arrival.Add(-offset), arrival)
	}
	if r.State != StateAligned || !r.Adjusted {
		t.Fatalf("state=%s adjusted=%v, want aligned+adjusted", r.State, r.Adjusted)
	}
	if got := a.Offset(); got < 30*time.Millisecond || got > 60*time.Millisecond {
		t.Fatalf("offset %v did not follow drift to ~49ms", got)
	}
	t.Logf("drift followed: offset=%v", a.Offset())
}

func TestSingleOutlierDoesNotCorruptEstimate(t *testing.T) {
	a := New(Config{})
	feed(a, base, 0, 5, 10*time.Millisecond)
	before := a.Offset()
	// 单条明显不合理的读数（偏 5s，在接受窗口之外、超前阈值之内 -> Outlier）
	arrival := base.Add(100 * time.Millisecond)
	r := a.Observe(arrival.Add(-5*time.Second), arrival)
	if r.Reason != ReasonOutlier {
		t.Fatalf("reason=%s, want Outlier", r.Reason)
	}
	if a.Offset() != before {
		t.Fatalf("offset moved by outlier: %v -> %v", before, a.Offset())
	}
	// 紧跟的正常读数不受影响
	r = a.Observe(arrival.Add(10*time.Millisecond), arrival.Add(10*time.Millisecond))
	if !r.Adjusted || r.Reason != ReasonNone {
		t.Fatalf("normal reading after outlier: adjusted=%v reason=%s", r.Adjusted, r.Reason)
	}
}

func TestDistinctAnomalyReasons(t *testing.T) {
	a := New(Config{})
	feed(a, base, 0, 5, 10*time.Millisecond)
	arrival := base.Add(100 * time.Millisecond)
	// 设备时钟明显超前 / 明显滞后 / 完全不合理，三者彼此可区分
	rAhead := a.Observe(arrival.Add(60*time.Second), arrival) // 设备钟快 60s
	if rAhead.Reason != ReasonAhead {
		t.Fatalf("fast clock reason=%s, want Ahead", rAhead.Reason)
	}
	rBehind := a.Observe(arrival.Add(-60*time.Second), arrival) // 设备钟慢 60s
	if rBehind.Reason != ReasonBehind {
		t.Fatalf("slow clock reason=%s, want Behind", rBehind.Reason)
	}
	rInvalid := a.Observe(time.Time{}, arrival)
	if rInvalid.Reason != ReasonInvalid {
		t.Fatalf("zero ts reason=%s, want Invalid", rInvalid.Reason)
	}
	// 异常样本不污染估计
	if a.Offset() != 0 {
		t.Fatalf("offset corrupted by anomalies: %v", a.Offset())
	}
	t.Logf("ahead=%s behind=%s invalid=%s", rAhead.Reason, rBehind.Reason, rInvalid.Reason)
}

func TestClockJumpReestablishes(t *testing.T) {
	a := New(Config{})
	feed(a, base, 0, 5, 10*time.Millisecond)
	// 时钟跳变：偏差突然变为 +10s 并持续
	var r Result
	jumped := false
	for i := 0; i < 3; i++ {
		arrival := base.Add(time.Duration(100+i*10) * time.Millisecond)
		r = a.Observe(arrival.Add(-10*time.Second), arrival)
		if r.Jump {
			jumped = true
		}
	}
	if !jumped {
		t.Fatal("expected jump to be detected after consecutive rejected samples")
	}
	if r.State != StateAligning {
		t.Fatalf("state=%s after jump, want Aligning", r.State)
	}
	// 重新建立后按新偏差对齐
	r = feed(a, base.Add(200*time.Millisecond), 10*time.Second, 3, 10*time.Millisecond)
	if r.State != StateAligned {
		t.Fatalf("state=%s after re-align, want Aligned", r.State)
	}
	arrival := base.Add(300 * time.Millisecond)
	res := a.Observe(arrival.Add(-10*time.Second), arrival)
	if !res.Adjusted || res.AlignedTS.Sub(arrival).Abs() > 5*time.Millisecond {
		t.Fatalf("post-jump alignment wrong: adjusted=%v err=%v", res.Adjusted, res.AlignedTS.Sub(arrival))
	}
	t.Logf("jump re-established: offset=%v", a.Offset())
}

func TestUnalignableDegradesAndRecovers(t *testing.T) {
	a := New(Config{AlignTimeoutMs: 100})
	// 样本互相矛盾（交替偏差），无法建立对齐
	for i := 0; i < 40; i++ {
		arrival := base.Add(time.Duration(i) * 10 * time.Millisecond)
		offset := time.Duration(i%2) * 5 * time.Second
		a.Observe(arrival.Add(-offset), arrival)
	}
	if a.State() != StateDegraded {
		t.Fatalf("state=%s, want Degraded", a.State())
	}
	// 恢复一致后应能重新对齐（持续一致样本冲刷环形缓冲）
	r := feed(a, base.Add(time.Second), time.Second, 40, 10*time.Millisecond)
	if r.State != StateAligned {
		t.Fatalf("state=%s after consistent samples, want Aligned", r.State)
	}
	t.Log("degraded -> recovered to aligned")
}

func TestResetDiscardsEstimate(t *testing.T) {
	a := New(Config{})
	feed(a, base, time.Second, 5, 10*time.Millisecond)
	a.Reset()
	if a.State() != StateAligning || a.Offset() != 0 {
		t.Fatalf("after reset: state=%s offset=%v", a.State(), a.Offset())
	}
}
