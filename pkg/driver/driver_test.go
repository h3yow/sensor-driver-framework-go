package driver

import (
	"errors"
	"net"
	"testing"
	"time"
)

// startSink 启动一个本地 TCP 服务，可选地周期性发送数据。
func startSink(t *testing.T, sendBytes []byte, interval time.Duration) (addr string, stop func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				if len(sendBytes) == 0 {
					<-done
					return
				}
				for {
					select {
					case <-done:
						return
					default:
					}
					if _, err := c.Write(sendBytes); err != nil {
						return
					}
					time.Sleep(interval)
				}
			}(c)
		}
	}()
	return ln.Addr().String(), func() { close(done); ln.Close() }
}

func TestLifecycleRejects(t *testing.T) {
	addr, stop := startSink(t, nil, 0)
	defer stop()

	s := New(addr)
	if s.State() != StateIdle {
		t.Fatalf("initial state = %s", s.State())
	}
	// 未连接读写被拒绝
	if _, err := s.Read(10); !errors.Is(err, ErrNotConnected) {
		t.Fatalf("read before connect: %v", err)
	}
	if err := s.Write([]byte("x")); !errors.Is(err, ErrNotConnected) {
		t.Fatalf("write before connect: %v", err)
	}
	// 未连接关闭被拒绝
	if err := s.Close(); !errors.Is(err, ErrNotConnected) {
		t.Fatalf("close before connect: %v", err)
	}
	// 正常连接
	if err := s.Connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}
	if s.State() != StateConnected {
		t.Fatalf("state after connect = %s", s.State())
	}
	// 重复启动被拒绝
	if err := s.Connect(); !errors.Is(err, ErrAlreadyStarted) {
		t.Fatalf("double connect: %v", err)
	}
	// 关闭
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if s.State() != StateClosed {
		t.Fatalf("state after close = %s", s.State())
	}
	// 重入关闭被拒绝
	if err := s.Close(); !errors.Is(err, ErrClosed) {
		t.Fatalf("double close: %v", err)
	}
	// 关闭后读写被拒绝
	if _, err := s.Read(10); !errors.Is(err, ErrClosed) {
		t.Fatalf("read after close: %v", err)
	}
}

func TestReconnect(t *testing.T) {
	addr, stop := startSink(t, []byte("ping"), 10*time.Millisecond)
	defer stop()

	s := New(addr)
	if err := s.Connect(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	// 重连
	if err := s.Connect(); err != nil {
		t.Fatalf("reconnect: %v", err)
	}
	if s.State() != StateConnected {
		t.Fatalf("state after reconnect = %s", s.State())
	}
	data, err := s.Read(500)
	if err != nil {
		t.Fatalf("read after reconnect: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("expected data after reconnect")
	}
	s.Close()
}

func TestReadTimeout(t *testing.T) {
	addr, stop := startSink(t, nil, 0) // 永不发送
	defer stop()

	s := New(addr)
	if err := s.Connect(); err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.Read(50); !errors.Is(err, ErrTimeout) {
		t.Fatalf("expected ErrTimeout, got %v", err)
	}
	// 超时后状态不卡死，仍可正常关闭
	if s.State() != StateConnected {
		t.Fatalf("state after timeout = %s", s.State())
	}
}

func TestConnectFailureNotStuck(t *testing.T) {
	// 连接一个未监听的地址
	s := New("127.0.0.1:1")
	if err := s.Connect(); err == nil {
		t.Fatal("expected connect error")
	}
	if s.State() != StateIdle {
		t.Fatalf("state after failed connect = %s, want Idle", s.State())
	}
	// 失败后可重试
	if err := s.Connect(); err == nil {
		t.Fatal("expected connect error again")
	}
}
