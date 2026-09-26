package session

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/h3yow/sensor-driver-framework-go/frame"
)

// pipeDialer 返回一个拨号即连通内存链路的 dialer，server 端持续读取存活。
func pipeDialer(t *testing.T) Dialer {
	t.Helper()
	d := func(ctx context.Context) (Conn, error) {
		c, s := net.Pipe()
		go func() {
			buf := make([]byte, 16)
			for {
				if _, err := s.Read(buf); err != nil {
					return
				}
			}
		}()
		return c, nil
	}
	return d
}

func waitForState(t *testing.T, s *Session, want State, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if s.State() == want {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("state=%s, want %s", s.State(), want)
}

func TestConnectLifecycleAndRejectRepeatedStart(t *testing.T) {
	d := pipeDialer(t)
	s := New(d, Options{})
	defer s.Close()

	if s.State() != StateIdle {
		t.Fatalf("initial state=%s want idle", s.State())
	}
	if err := s.Connect(context.Background()); err != nil {
		t.Fatalf("connect: %v", err)
	}
	waitForState(t, s, StateConnected, time.Second)

	if err := s.Connect(context.Background()); !errors.Is(err, ErrAlreadyStarted) {
		t.Fatalf("repeat connect err=%v, want ErrAlreadyStarted", err)
	}
	if err := s.Reconnect(context.Background()); !errors.Is(err, ErrReconnectState) {
		t.Fatalf("reconnect while connected err=%v, want ErrReconnectState", err)
	}
}

func TestReadWriteWhenNotConnected(t *testing.T) {
	d := pipeDialer(t)
	s := New(d, Options{})
	defer s.Close()

	// Idle 状态读写必须被拒绝。
	if err := s.Write(context.Background(), []byte{1}); !errors.Is(err, ErrNotConnected) {
		t.Fatalf("idle write err=%v want ErrNotConnected", err)
	}
	if err := s.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitForState(t, s, StateConnected, time.Second)
	if err := s.Write(context.Background(), []byte("ping")); err != nil {
		t.Fatalf("connected write: %v", err)
	}
}

func TestCloseIdempotentAndRejectsFurtherOps(t *testing.T) {
	d := pipeDialer(t)
	s := New(d, Options{})
	if err := s.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitForState(t, s, StateConnected, time.Second)

	if err := s.Close(); err != nil {
		t.Fatalf("first close: %v", err)
	}
	if err := s.Close(); !errors.Is(err, ErrClosed) {
		t.Fatalf("second close err=%v want ErrClosed", err)
	}
	if err := s.Write(context.Background(), []byte{1}); !errors.Is(err, ErrClosed) {
		t.Fatalf("write after close err=%v want ErrClosed", err)
	}
	if err := s.Connect(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("connect after close err=%v want ErrClosed", err)
	}
	if err := s.Reconnect(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("reconnect after close err=%v want ErrClosed", err)
	}
}

func TestDialFailureLeavesDisconnected(t *testing.T) {
	s := New(func(ctx context.Context) (Conn, error) {
		return nil, errors.New("network unreachable")
	}, Options{DialTimeout: time.Second})
	defer s.Close()

	err := s.Connect(context.Background())
	if !errors.Is(err, ErrDialFailed) {
		t.Fatalf("err=%v want ErrDialFailed", err)
	}
	if s.State() != StateDisconnected {
		t.Fatalf("state=%s want disconnected", s.State())
	}
	// Disconnected 允许重连；再次拨号失败应仍返回 ErrDialFailed。
	if err := s.Reconnect(context.Background()); !errors.Is(err, ErrDialFailed) {
		t.Fatalf("reconnect err=%v want ErrDialFailed", err)
	}
}

func TestReconnectAfterServerClose(t *testing.T) {
	var mu sync.Mutex
	serverConns := make([]net.Conn, 0, 2)
	d := func(ctx context.Context) (Conn, error) {
		c, srv := net.Pipe()
		mu.Lock()
		serverConns = append(serverConns, srv)
		mu.Unlock()
		// 第一次连接在 50ms 后被服务端关闭，之后的连接保持存活。
		attempt := len(serverConns)
		go func() {
			buf := make([]byte, 16)
			if attempt == 1 {
				time.Sleep(50 * time.Millisecond)
				_ = srv.Close()
				return
			}
			for {
				if _, err := srv.Read(buf); err != nil {
					return
				}
			}
		}()
		return c, nil
	}

	s := New(d, Options{ReadIdle: 200 * time.Millisecond})
	defer s.Close()
	if err := s.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitForState(t, s, StateConnected, time.Second)
	waitForState(t, s, StateDisconnected, time.Second)

	// Disconnected 之外的状态重连会被拒绝（这里恰好在 disconnected）。
	if err := s.Reconnect(context.Background()); err != nil {
		t.Fatalf("reconnect: %v", err)
	}
	waitForState(t, s, StateConnected, time.Second)
}

func TestReadLoopParsesFramesAndReportsBadFrames(t *testing.T) {
	goodCh := make(chan *frame.Frame, 4)
	badCh := make(chan *frame.ParseError, 4)
	states := make(chan State, 8)

	d := func(ctx context.Context) (Conn, error) {
		c, srv := net.Pipe()
		go func() {
			defer srv.Close()
			good, _ := frame.Encode(&frame.Frame{Type: frame.TypeReading, Seq: 9, TS: 111, Payload: make([]byte, 8)})
			good2, _ := frame.Encode(&frame.Frame{Type: frame.TypeReading, Seq: 10, TS: 222, Payload: make([]byte, 8)})
			corrupt := append([]byte(nil), good...)
			corrupt[len(corrupt)-1] ^= 0xFF
			_, _ = srv.Write(append(append(good, corrupt...), good2...))
			buf := make([]byte, 16)
			for {
				if _, err := srv.Read(buf); err != nil {
					return
				}
			}
		}()
		return c, nil
	}

	s := New(d, Options{
		Hooks: Hooks{OnEvent: func(ev Event) {
			switch ev.Kind {
			case EvGoodFrame:
				goodCh <- ev.Frame
			case EvBadFrame:
				badCh <- ev.Bad
			case EvStateChanged:
				states <- ev.State
			}
		}},
	})
	defer s.Close()
	if err := s.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}

	f1 := <-goodCh
	if f1.Seq != 9 {
		t.Fatalf("first good frame seq=%d", f1.Seq)
	}
	bad := <-badCh
	if bad.Kind != frame.ErrCRC {
		t.Fatalf("bad kind=%s want crc_mismatch", bad.Kind)
	}
	f2 := <-goodCh
	if f2.Seq != 10 {
		t.Fatalf("second good frame seq=%d, parser must recover after bad frame", f2.Seq)
	}
}
