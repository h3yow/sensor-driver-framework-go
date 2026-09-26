package sensor

import (
	"testing"
	"time"
)

func spec() Spec {
	return Spec{Kind: KindTemperature, Min: -40, Max: 80, MaxAge: 5 * time.Second}
}

func rd(seq uint16, devTS, recvTS int64, v float64) Reading {
	return Reading{Kind: KindTemperature, Seq: seq, DeviceTS: devTS, RecvTS: recvTS, Value: v}
}

func TestAcceptInRangeOrdered(t *testing.T) {
	v := NewValidator(spec())
	base := time.Now().UnixMilli()
	r1, ok1 := v.Judge(rd(1, base, base+10, 25.0))
	r2, ok2 := v.Judge(rd(2, base+100, base+110, 26.0))
	if !ok1 || !ok2 || r1.Verdict != VerdictOK || r2.Verdict != VerdictOK {
		t.Fatalf("expected both accepted")
	}
}

func TestRejectOutOfRange(t *testing.T) {
	v := NewValidator(spec())
	base := time.Now().UnixMilli()
	for _, val := range []float64{-40.1, 80.1, 1e6} {
		if r, ok := v.Judge(rd(1, base, base, val)); ok || r.Verdict != VerdictOutOfRange {
			t.Fatalf("value %v must be out_of-range, got ok=%v verdict=%s", val, ok, r.Verdict)
		}
	}
	// 边界值合法。
	if r, ok := v.Judge(rd(2, base+1, base+1, -40)); !ok || r.Verdict != VerdictOK {
		t.Fatalf("boundary value must be accepted")
	}
}

func TestRejectOutOfOrder(t *testing.T) {
	v := NewValidator(spec())
	base := time.Now().UnixMilli()
	if _, ok := v.Judge(rd(1, base+1000, base+1000, 25)); !ok {
		t.Fatal("first must accept")
	}
	// 更早的设备时间戳 -> 乱序。
	r, ok := v.Judge(rd(2, base+500, base+1100, 25))
	if ok || r.Verdict != VerdictOutOfOrder {
		t.Fatalf("older ts must be out_of_order, got ok=%v verdict=%s", ok, r.Verdict)
	}
	// 被拒读数不推进基线：下一条时间戳正常的读数仍应接受。
	if _, ok := v.Judge(rd(3, base+2000, base+2000, 25)); !ok {
		t.Fatal("rejected reading must not advance baseline")
	}
}

func TestRejectStale(t *testing.T) {
	v := NewValidator(spec())
	now := time.Now().UnixMilli()
	r, ok := v.Judge(rd(1, now-6000, now, 25))
	if ok || r.Verdict != VerdictStale {
		t.Fatalf("old reading must be stale, ok=%v verdict=%s", ok, r.Verdict)
	}
	// 设备时间戳超前过多同样拒绝。
	r2, ok2 := v.Judge(rd(2, now+60000, now, 25))
	if ok2 || r2.Verdict != VerdictStale {
		t.Fatalf("future reading must be stale, ok=%v verdict=%s", ok2, r2.Verdict)
	}
}

func TestAllowReorder(t *testing.T) {
	sp := spec()
	sp.AllowReorder = true
	v := NewValidator(sp)
	base := time.Now().UnixMilli()
	_, _ = v.Judge(rd(1, base+1000, base+1000, 25))
	if _, ok := v.Judge(rd(2, base+500, base+1100, 25)); !ok {
		t.Fatal("reorder should be allowed by spec")
	}
}

func TestResetClearsBaseline(t *testing.T) {
	v := NewValidator(spec())
	base := time.Now().UnixMilli()
	_, _ = v.Judge(rd(1, base+1000, base+1000, 25))
	v.Reset()
	// Reset 后，第一条读数即使设备时间戳较小也不能被判乱序。
	if _, ok := v.Judge(rd(2, base, base, 25)); !ok {
		t.Fatal("after reset baseline must be cleared")
	}
}

func TestIsUsableAging(t *testing.T) {
	sp := spec()
	now := int64(100_000)
	r := Reading{Verdict: VerdictOK, DeviceTS: now - 6000}
	if IsUsable(r, now, sp) {
		t.Fatal("aged reading must not be usable")
	}
	r.DeviceTS = now - 100
	if !IsUsable(r, now, sp) {
		t.Fatal("fresh reading must be usable")
	}
	r.Verdict = VerdictOutOfRange
	if IsUsable(r, now, sp) {
		t.Fatal("rejected verdict must not be usable")
	}
}
