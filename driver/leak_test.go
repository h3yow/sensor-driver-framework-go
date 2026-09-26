package driver

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/h3yow/sensor-driver-framework-go/sim"
)

func TestNoGoroutineLeakAfterStop(t *testing.T) {
	runtime.GC()
	baseline := runtime.NumGoroutine()
	clock := newFakeClock()
	m := NewManager(WithClock(clock))
	for i := 0; i < 3; i++ {
		id := string(rune('a' + i))
		dev := sim.New(func(attempt int) sim.Script {
			return sim.Script{Steps: []sim.Step{{Kind: sim.StepBlock}}}
		})
		_ = m.Register(DeviceConfig{
			ID: id, Dialer: dev.Dialer(), Spec: tempSpec(),
			IdleTimeout: 30 * time.Millisecond, MaxStalls: 2, Reconnect: fastReconnect(),
		})
	}
	_ = m.Start(context.Background())
	time.Sleep(300 * time.Millisecond) // 让若干次隔离/重连发生
	m.Stop()

	// 停止后反复 GC 并采样 goroutine 数量，应稳定回落到启动前基线附近。
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		runtime.GC()
		time.Sleep(20 * time.Millisecond)
		if n := runtime.NumGoroutine(); n <= baseline {
			baseline = n
		}
	}
	final := runtime.NumGoroutine()
	if final > baseline+2 {
		t.Fatalf("goroutine leak: before=%d after=%d", baseline, final)
	}
}
