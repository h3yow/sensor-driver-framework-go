package sim

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/h3yow/sensor-driver-framework-go/frame"
)

func readFull(t *testing.T, conn interface {
	Read(p []byte) (int, error)
}, n int) []byte {
	t.Helper()
	out := make([]byte, n)
	got := 0
	deadline := time.Now().Add(2 * time.Second)
	for got < n {
		if time.Now().After(deadline) {
			t.Fatalf("timeout reading, got %d/%d", got, n)
		}
		nn, err := conn.Read(out[got:])
		got += nn
		if err == io.EOF {
			break
		}
	}
	return out[:got]
}

func TestHalfPacketWritesArriveInTwoPieces(t *testing.T) {
	fr := frame.Frame{Type: frame.TypeReading, Seq: 3, TS: 9, Payload: []byte("01234567")}
	raw, _ := frame.Encode(&fr)

	dev := New(func(attempt int) Script {
		return Script{Steps: []Step{
			{Kind: StepSendFrame, Frame: fr, Split: true, SplitDelay: 30 * time.Millisecond},
			{Kind: StepBlock},
		}}
	})
	conn, err := dev.Dialer()(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	buf := make([]byte, len(raw))
	// 第一半应在 30ms 后仍只到达一半左右。
	conn.SetReadDeadline(time.Now().Add(20 * time.Millisecond))
	n1, _ := conn.Read(buf)
	if n1 <= 0 || n1 >= len(raw) {
		t.Fatalf("first piece size=%d, want 0..%d", n1, len(raw)-1)
	}
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	n2, err := conn.Read(buf[n1:])
	if err != nil || n1+n2 != len(raw) {
		t.Fatalf("second piece n2=%d err=%v total=%d want %d", n2, err, n1+n2, len(raw))
	}
	if string(buf) != string(raw) {
		t.Fatal("reassembled bytes mismatch")
	}
}

func TestStickyPacketsArriveInOneWrite(t *testing.T) {
	a := frame.Frame{Type: frame.TypeReading, Seq: 1, TS: 1, Payload: []byte("aaaaaaaa")}
	b := frame.Frame{Type: frame.TypeReading, Seq: 2, TS: 2, Payload: []byte("bbbbbbbb")}
	rawA, _ := frame.Encode(&a)
	rawB, _ := frame.Encode(&b)

	dev := New(func(attempt int) Script {
		return Script{Steps: []Step{
			{Kind: StepSendFrame, Frame: a, PackWithNext: true},
			{Kind: StepSendFrame, Frame: b},
			{Kind: StepBlock},
		}}
	})
	conn, _ := dev.Dialer()(context.Background())
	defer conn.Close()

	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, len(rawA)+len(rawB))
	_, err := io.ReadFull(conn, buf)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(buf[:len(rawA)]) != string(rawA) || string(buf[len(rawA):]) != string(rawB) {
		t.Fatal("sticky payload mismatch")
	}
}

func TestBlockStepKeepsConnectionSilent(t *testing.T) {
	dev := New(func(attempt int) Script {
		return Script{Steps: []Step{{Kind: StepBlock}}}
	})
	conn, _ := dev.Dialer()(context.Background())
	defer conn.Close()

	conn.SetReadDeadline(time.Now().Add(80 * time.Millisecond))
	buf := make([]byte, 8)
	if _, err := conn.Read(buf); err == nil {
		t.Fatal("blocked device must not deliver data")
	}
	// 连接仍未关闭：对端 goroutine 存活，等待客户端关闭。
	if dev.Attempts() != 1 {
		t.Fatalf("attempts=%d want 1", dev.Attempts())
	}
}

func TestBehaviorPerAttempt(t *testing.T) {
	dev := New(func(attempt int) Script {
		if attempt == 1 {
			return Script{Steps: []Step{{Kind: StepClose}}}
		}
		fr := frame.Frame{Type: frame.TypeReading, Seq: 1, TS: 1, Payload: make([]byte, 8)}
		return Script{Steps: []Step{
			{Kind: StepSendFrame, Frame: fr},
			{Kind: StepBlock},
		}}
	})
	dialer := dev.Dialer()

	c1, err := dialer(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	c1.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	buf := make([]byte, 1)
	if _, err := c1.Read(buf); err != io.EOF {
		t.Fatalf("attempt1 should close, read err=%v", err)
	}
	c1.Close()

	c2, err := dialer(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	c2.SetReadDeadline(time.Now().Add(time.Second))
	raw := make([]byte, frame.HeaderSize+8)
	if _, err := io.ReadFull(c2, raw); err != nil {
		t.Fatalf("attempt2 should send a frame, got %v", err)
	}
	if dev.Attempts() != 2 {
		t.Fatalf("attempts=%d want 2", dev.Attempts())
	}
}
