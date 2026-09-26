package simdev

import (
	"net"
	"testing"
	"time"

	"github.com/h3yow/sensor-driver-framework-go/pkg/frame"
)

// collect 从设备连接读取并解析出 n 帧（或超时返回已解析部分）。
func collect(t *testing.T, d *Device, n int, timeout time.Duration) []frame.Frame {
	t.Helper()
	conn, err := net.DialTimeout("tcp", d.Addr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	dec := frame.NewDecoder(3)
	var out []frame.Frame
	deadline := time.Now().Add(timeout)
	for len(out) < n && time.Now().Before(deadline) {
		_ = conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		buf := make([]byte, 4096)
		m, err := conn.Read(buf)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			break
		}
		out = append(out, dec.Feed(buf[:m])...)
	}
	return out
}

func TestIdealDevice(t *testing.T) {
	d, err := New("ideal", Behavior{}, 5)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	frames := collect(t, d, 5, 2*time.Second)
	if len(frames) != 5 {
		t.Fatalf("got %d frames, want 5", len(frames))
	}
	for i, f := range frames {
		ts, v, ok := DecodePayload(f.Payload)
		if !ok {
			t.Fatalf("frame %d bad payload", i)
		}
		if want := 20.0 + float64(i%10); v != want {
			t.Fatalf("frame %d value = %v, want %v", i, v, want)
		}
		if time.Since(ts) > 5*time.Second {
			t.Fatalf("frame %d timestamp too old: %v", i, ts)
		}
	}
}

func TestSplitAndSticky(t *testing.T) {
	d, err := New("split", Behavior{SplitFrames: true, Sticky: true}, 6)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	frames := collect(t, d, 6, 3*time.Second)
	if len(frames) != 6 {
		t.Fatalf("split+sticky: got %d frames, want 6", len(frames))
	}
}

func TestCorruptInjection(t *testing.T) {
	d, err := New("corrupt", Behavior{CorruptEvery: 2}, 6)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	// 每 2 帧坏 1 帧：6 帧中 3 好 3 坏
	frames := collect(t, d, 3, 3*time.Second)
	if len(frames) != 3 {
		t.Fatalf("corrupt: got %d good frames, want 3", len(frames))
	}
}

func TestReorder(t *testing.T) {
	d, err := New("reorder", Behavior{ReorderWindow: 2}, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	frames := collect(t, d, 4, 3*time.Second)
	if len(frames) != 4 {
		t.Fatalf("reorder: got %d frames, want 4", len(frames))
	}
	// 窗口反转：seq 应为 1,0,3,2
	want := []uint16{1, 0, 3, 2}
	for i, f := range frames {
		if f.Seq != want[i] {
			t.Fatalf("frame %d seq = %d, want %d", i, f.Seq, want[i])
		}
	}
}

func TestBlockingDevice(t *testing.T) {
	d, err := New("blocker", Behavior{BlockAfter: 2, BlockMs: 300}, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	start := time.Now()
	frames := collect(t, d, 4, 3*time.Second)
	if len(frames) != 4 {
		t.Fatalf("blocker: got %d frames, want 4", len(frames))
	}
	if elapsed := time.Since(start); elapsed < 250*time.Millisecond {
		t.Fatalf("expected blocking delay, elapsed=%v", elapsed)
	}
}
