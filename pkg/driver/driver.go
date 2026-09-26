// Package driver 实现单设备会话的生命周期状态机。
//
// 生命周期: Idle -> Connecting -> Connected -> Closing -> Closed
// Closed 之后允许再次 Connect（重连），状态重新进入 Connecting。
// 重复启动、重入关闭、未连接读写均被拒绝并返回可区分的错误。
package driver

import (
	"errors"
	"log"
	"net"
	"sync"
	"time"
)

// State 表示会话生命周期状态。
type State int

const (
	StateIdle State = iota
	StateConnecting
	StateConnected
	StateClosing
	StateClosed
)

func (s State) String() string {
	switch s {
	case StateIdle:
		return "Idle"
	case StateConnecting:
		return "Connecting"
	case StateConnected:
		return "Connected"
	case StateClosing:
		return "Closing"
	case StateClosed:
		return "Closed"
	}
	return "Unknown"
}

// 可区分的失败原因。
var (
	ErrAlreadyStarted = errors.New("driver: session already started")
	ErrNotConnected   = errors.New("driver: not connected")
	ErrClosed         = errors.New("driver: session closed")
	ErrTimeout        = errors.New("driver: io timeout")
)

// Session 管理一条到设备的 TCP 连接，所有状态迁移均有日志。
type Session struct {
	addr  string
	mu    sync.Mutex
	state State
	conn  net.Conn
}

// New 创建一个空闲会话。
func New(addr string) *Session {
	return &Session{addr: addr, state: StateIdle}
}

// transition 在持锁状态下迁移状态并打印日志。
func (s *Session) transition(to State) {
	log.Printf("[driver] addr=%s state %s -> %s", s.addr, s.state, to)
	s.state = to
}

// Connect 建立连接；重复启动返回 ErrAlreadyStarted；允许从 Closed 重连。
func (s *Session) Connect() error {
	s.mu.Lock()
	switch s.state {
	case StateConnecting, StateConnected:
		s.mu.Unlock()
		return ErrAlreadyStarted
	}
	s.transition(StateConnecting)
	s.mu.Unlock()

	conn, err := net.DialTimeout("tcp", s.addr, 2*time.Second)

	s.mu.Lock()
	defer s.mu.Unlock()
	if err != nil {
		// 失败不卡死：回到 Idle（首次）或 Closed（重连失败保持 Closed 语义统一为 Idle 亦可，此处选择 Idle 以便重试）。
		s.transition(StateIdle)
		return err
	}
	s.conn = conn
	s.transition(StateConnected)
	return nil
}

// Close 关闭连接；未连接返回 ErrNotConnected，重复关闭返回 ErrClosed。
// 并发安全：仅首次生效。
func (s *Session) Close() error {
	s.mu.Lock()
	switch s.state {
	case StateIdle:
		s.mu.Unlock()
		return ErrNotConnected
	case StateClosed:
		s.mu.Unlock()
		return ErrClosed
	case StateClosing:
		s.mu.Unlock()
		return ErrClosing
	}
	s.transition(StateClosing)
	conn := s.conn
	s.conn = nil
	s.mu.Unlock()

	if conn != nil {
		_ = conn.Close()
	}

	s.mu.Lock()
	s.transition(StateClosed)
	s.mu.Unlock()
	return nil
}

// ErrClosing 表示关闭操作正在进行中（并发重入关闭）。
var ErrClosing = errors.New("driver: close already in progress")

// checkIO 校验当前状态是否允许读写。
func (s *Session) checkIO() (net.Conn, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch s.state {
	case StateConnected:
		return s.conn, nil
	case StateClosed:
		return nil, ErrClosed
	default:
		return nil, ErrNotConnected
	}
}

// Read 读取数据；未连接时返回 ErrNotConnected/ErrClosed，超时返回 ErrTimeout。
func (s *Session) Read(timeoutMs int) ([]byte, error) {
	conn, err := s.checkIO()
	if err != nil {
		return nil, err
	}
	if timeoutMs > 0 {
		_ = conn.SetReadDeadline(time.Now().Add(time.Duration(timeoutMs) * time.Millisecond))
	}
	buf := make([]byte, 4096)
	n, err := conn.Read(buf)
	if err != nil {
		if ne, ok := err.(net.Error); ok && ne.Timeout() {
			return nil, ErrTimeout
		}
		return nil, err
	}
	return buf[:n], nil
}

// Write 写入数据；未连接时返回 ErrNotConnected/ErrClosed。
func (s *Session) Write(p []byte) error {
	conn, err := s.checkIO()
	if err != nil {
		return err
	}
	_, err = conn.Write(p)
	return err
}

// State 返回当前生命周期状态。
func (s *Session) State() State {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state
}
