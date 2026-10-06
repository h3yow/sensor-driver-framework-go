package clockalign

import (
	"testing"
	"time"
)

var t0 = time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)

// feed 以固定间隔喂入 rawTS 序列，返回最后一次结果。
func feed(d *DeviceClock, start time.Time, step time.Duration, raws ...time.Time) Result {
	var res Result
	for i, r := range raws {
		res = d.Align(r, start.Add(time.Duration(i)*step))
	}
	return res
}

// 设备时钟稳定偏慢 10s：应建立对齐并修正到统一时间轴。
func TestAlignConsistentOffset(t *testing.T) {
	d := New(Config{MinSamples: 3})
	offset := -10 * time.Second // 设备钟慢 10s
	var res Result
	for i := 0; i < 5; i++ {
		arrival := t0.Add(time.Duration(i) * 100 * time.Millisecond)
		raw := arrival.Add(offset)
		res = d.Align(raw, arrival)
		t.Logf("i=%d raw=%s anomaly=%s aligned=%v adjusted=%v", i, raw.Format(time.RFC3339Nano), res.Anomaly, res.Aligned, res.Adjusted)
	}
	if !res.Aligned {
		t.Fatal("device should be aligned")
	}
	if !res.Adjusted {
		t.Fatal("reading should be marked adjusted")
	}
	// 对齐后的时间应接近到达时刻（统一时间轴）
	last := t0.Add(4 * 100 * time.Millisecond)
	if diff := res.AlignedTS.Sub(last); diff < -200*time.Millisecond || diff > 200*time.Millisecond {
		t.Fatalf("aligned ts off by %v", diff)
	}
	off, ok := d.Offset()
	if !ok {
		t.Fatal("offset should be available")
	}
	if off < 9*time.Second || off > 11*time.Second {
		t.Fatalf("estimated offset %v, want ~10s", off)
	}
	t.Logf("estimated offset=%v", off)
}

// 缓慢漂移：偏差估计应跟上。
func TestAlignFollowsDrift(t *testing.T) {
	d := New(Config{MinSamples: 3, Alpha: 0.5})
	for i := 0; i < 40; i++ {
		arrival := t0.Add(time.Duration(i) * 100 * time.Millisecond)
		drift := time.Duration(i) * 50 * time.Millisecond // 每拍漂移 50ms
		d.Align(arrival.Add(-drift), arrival)
	}
	off, ok := d.Offset()
	if !ok {
		t.Fatal("should be aligned")
	}
	want := 39 * 50 * time.Millisecond
	if diff := off - want; diff < -300*time.Millisecond || diff > 300*time.Millisecond {
		t.Fatalf("offset %v did not follow drift to ~%v", off, want)
	}
	t.Logf("offset followed drift to %v (want ~%v)", off, want)
}

// 单条明显离群的读数不应带偏估计。
func TestOutlierDoesNotCorruptEstimate(t *testing.T) {
	d := New(Config{MinSamples: 3})
	for i := 0; i < 5; i++ {
		arrival := t0.Add(time.Duration(i) * 100 * time.Millisecond)
		d.Align(arrival.Add(-time.Second), arrival) // 稳定偏慢 1s
	}
	before, _ := d.Offset()
	// 离群：偏慢 30s（超出 MaxLagMs）
	arrival := t0.Add(600 * time.Millisecond)
	res := d.Align(arrival.Add(-30*time.Second), arrival)
	if res.Anomaly != AnomalyBehind {
		t.Fatalf("outlier judged %s, want Behind", res.Anomaly)
	}
	after, _ := d.Offset()
	if after != before {
		t.Fatalf("estimate moved by outlier: %v -> %v", before, after)
	}
	t.Logf("estimate unchanged by outlier: %v", after)
}

// 明显超前：对齐后一条远超估计的读数应判为 Future。
func TestFutureAnomaly(t *testing.T) {
	d := New(Config{MinSamples: 3, MaxFutureMs: 2000})
	for i := 0; i < 5; i++ {
		arrival := t0.Add(time.Duration(i) * 100 * time.Millisecond)
		d.Align(arrival, arrival)
	}
	arrival := t0.Add(600 * time.Millisecond)
	res := d.Align(arrival.Add(10*time.Second), arrival)
	if res.Anomaly != AnomalyFuture {
		t.Fatalf("judged %s, want Future", res.Anomaly)
	}
	if res.Adjusted {
		t.Fatal("anomalous reading must not be adjusted")
	}
	t.Logf("future anomaly detected: %s", res.Anomaly)
}

// 完全不合理：1970 年的时间戳应判为 Invalid。
func TestInvalidAnomaly(t *testing.T) {
	d := New(Config{})
	res := d.Align(time.Unix(5, 0), t0)
	if res.Anomaly != AnomalyInvalid {
		t.Fatalf("judged %s, want Invalid", res.Anomaly)
	}
	t.Logf("invalid timestamp detected: %s", res.Anomaly)
}

// 运行中时间跳变：连续异常达到阈值后重建，随后按新偏差对齐。
func TestJumpTriggersRebuild(t *testing.T) {
	d := New(Config{MinSamples: 3, ResetThreshold: 3})
	for i := 0; i < 5; i++ {
		arrival := t0.Add(time.Duration(i) * 100 * time.Millisecond)
		d.Align(arrival, arrival)
	}
	// 时钟跳变：设备钟突然慢 1 小时
	var res Result
	for i := 5; i < 12; i++ {
		arrival := t0.Add(time.Duration(i) * 100 * time.Millisecond)
		res = d.Align(arrival.Add(-time.Hour), arrival)
		t.Logf("i=%d anomaly=%s reset=%v aligned=%v", i, res.Anomaly, res.Reset, res.Aligned)
	}
	if !res.Aligned {
		t.Fatal("should re-align after jump")
	}
	off, _ := d.Offset()
	if off < 59*time.Minute || off > 61*time.Minute {
		t.Fatalf("offset %v, want ~1h after rebuild", off)
	}
	t.Logf("rebuilt offset=%v", off)
}

// 重启后从零重新计时：1970 纪元连续出现应触发纪元重置并重新对齐。
func TestEpochRestartFromZero(t *testing.T) {
	d := New(Config{MinSamples: 3, ResetThreshold: 3})
	for i := 0; i < 5; i++ {
		arrival := t0.Add(time.Duration(i) * 100 * time.Millisecond)
		d.Align(arrival, arrival)
	}
	// 设备重启：时间戳从 1970 起算
	boot := time.Unix(0, 0)
	var res Result
	for i := 5; i < 12; i++ {
		arrival := t0.Add(time.Duration(i) * 100 * time.Millisecond)
		raw := boot.Add(time.Duration(i-5) * 100 * time.Millisecond)
		res = d.Align(raw, arrival)
		t.Logf("i=%d raw=%s anomaly=%s reset=%v aligned=%v adjusted=%v",
			i, raw.Format(time.RFC3339Nano), res.Anomaly, res.Reset, res.Aligned, res.Adjusted)
	}
	if !res.Aligned {
		t.Fatal("should re-align after epoch restart")
	}
	// 对齐后时间应回到统一时间轴附近
	last := t0.Add(11 * 100 * time.Millisecond)
	if diff := res.AlignedTS.Sub(last); diff < -time.Second || diff > time.Second {
		t.Fatalf("aligned ts off by %v after epoch restart", diff)
	}
	t.Logf("re-aligned after epoch restart, alignedTS=%s", res.AlignedTS.Format(time.RFC3339Nano))
}

// 异常读数后的正常读数不应被带偏（估计不变，正常读数仍判正常）。
func TestNormalAfterAnomalyStillGood(t *testing.T) {
	d := New(Config{MinSamples: 3})
	for i := 0; i < 5; i++ {
		arrival := t0.Add(time.Duration(i) * 100 * time.Millisecond)
		d.Align(arrival, arrival)
	}
	// 一条异常（超前 10s）
	d.Align(t0.Add(600*time.Millisecond).Add(10*time.Second), t0.Add(600*time.Millisecond))
	// 紧跟的正常读数
	arrival := t0.Add(700 * time.Millisecond)
	res := d.Align(arrival, arrival)
	if res.Anomaly != AnomalyNone {
		t.Fatalf("normal reading after anomaly judged %s", res.Anomaly)
	}
	t.Logf("normal reading after anomaly judged %s", res.Anomaly)
}

// 长时间无法对齐：持续异常且超时后应上报 Unaligned。
func TestUnalignedTimeout(t *testing.T) {
	d := New(Config{MinSamples: 3, ResetThreshold: 100, AlignTimeoutMs: 1000})
	d.sinceReset = t0 // 直接构造“早已开始尝试”的状态
	// 持续完全不合理的时间戳（1970 纪元）：永远无法攒够有效样本
	var res Result
	for i := 0; i < 4; i++ {
		arrival := t0.Add(2*time.Second + time.Duration(i)*100*time.Millisecond)
		raw := time.Unix(int64(i), 0)
		res = d.Align(raw, arrival)
		t.Logf("i=%d anomaly=%s unaligned=%v", i, res.Anomaly, res.Unaligned)
	}
	if !res.Unaligned {
		t.Fatal("should report unaligned after timeout with persistent anomalies")
	}
}
