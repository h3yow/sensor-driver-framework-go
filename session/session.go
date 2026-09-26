// Package session 实现单个传感器设备的连接会话与确定的生命周期状态机。
//
// 状态迁移：
//
//	Idle --Connect--> Connecting --dial ok--> Connected
//	                          \--dial err--> Disconnected
//	Connected --读错误/写错误/空闲超时--> Disconnected
//	Disconnected --Reconnect--> Connecting --> Connected | Disconnected
//	任意状态 --Close--> Closed（终态）
//
// 并发与故障隔离：
//   - 重复 Connect 返回 ErrAlreadyStarted；仅 Disconnected 允许 Reconnect；
//     未连接时读写返回 ErrNotConnected；Closed 之后一切操作返回 ErrClosed。
//   - 读循环发生 panic 会被 recover 并转为 Disconnected，绝不崩溃进程；
//   - 每次连接使用全新的 frame.Decoder，重连不携带半包等旧解析状态。
package session

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/h3yow/sensor-driver-framework-go/frame"
)

// State 是会话生命周期状态。
type State int

const (
	StateIdle State = iota // 初始/未连接
	StateConnecting
	StateConnected
	StateDisconnected // 发生过故障、允许重连
	StateClosed       // 永久关闭，不可再用
)

func (s State) String() string {
	switch s {
	case StateIdle:
		return "idle"
	case StateConnecting:
		return "connecting"
	case StateConnected:
		return "connected"
	case StateDisconnected:
		return "disconnected"
	case StateClosed:
		return "closed"
	default:
		return "unknown"
	}
}

// Dialer 建立一次底层连接。
type Dialer func(ctx context.Context) (Conn, error)

// Conn 是会话所依赖的最小传输抽象。
type Conn interface {
	Read(p []byte) (int, error)
	Write(p []byte) (int, error)
	SetReadDeadline(t time.Time) error
	SetWriteDeadline(t time.Time) error
	Close() error
}

// Event 表示一次可观察的会话事件（供上层做恢复动作）。
type Event struct {
	Kind   string
	Detail string
	Frame  *frame.Frame
	Bad    *frame.ParseError
	State  State // EvStateChanged 时的新状态
}

const (
	EvStateChanged = "state_changed"
	EvGoodFrame    = "good_frame"
	EvBadFrame     = "bad_frame"
	EvIdleTimeout  = "idle_timeout"
	EvReadError    = "read_error"
)

// Hooks 为上层注入的事件回调（可为 nil）。
type Hooks struct {
	OnEvent func(ev Event)
}

// Options 配置会话行为。
type Options struct {
	DialTimeout  time.Duration
	ReadIdle     time.Duration // 读取空闲超时，0 表示不启用
	WriteTimeout time.Duration
	Hooks        Hooks
}

// Session 是单设备会话。
type Session struct {
	dialer Dialer
	opts   Options

	mu          sync.Mutex
	state       State
	conn        Conn
	connectedAt time.Time
	readDone    chan struct{} // 当前读循环结束信号；nil 表示无读循环
	closeOnce   sync.Once
}

// New 创建会话，此时处于 StateIdle，不发起任何连接。
func New(dialer Dialer, opts Options) *Session {
	if opts.DialTimeout <= 0 {
		opts.DialTimeout = 3 * time.Second
	}
	if opts.WriteTimeout <= 0 {
		opts.WriteTimeout = 2 * time.Second
	}
	return &Session{dialer: dialer, opts: opts, state: StateIdle}
}

func (s *Session) emit(ev Event) {
	if s.opts.Hooks.OnEvent != nil {
		// 回调在锁外执行，避免上层逻辑反向取锁造成死锁。
		s.opts.Hooks.OnEvent(ev)
	}
}

func (s *Session) setStateLocked(st State) {
	if s.state == st {
		return
	}
	s.state = st
	s.emit(Event{Kind: EvStateChanged, State: st})
}

// connectLocked 在持有锁的前提下完成拨号并启动读循环。
// 调用返回时若成功，读循环已启动；失败则状态为 Disconnected。
func (s *Session) connectLocked(ctx context.Context) error {
	if s.dialer == nil {
		s.setStateLocked(StateDisconnected)
		return ErrNilDialer
	}
	s.setStateLocked(StateConnecting)

	dialCtx, cancel := context.WithTimeout(ctx, s.opts.DialTimeout)
	defer cancel()
	conn, err := s.dialer(dialCtx)
	if err != nil {
		s.setStateLocked(StateDisconnected)
		return errors.Join(ErrDialFailed, err)
	}

	s.conn = conn
	s.connectedAt = time.Now()
	s.readDone = make(chan struct{})
	s.setStateLocked(StateConnected)
	go s.readLoop(conn, s.readDone)
	return nil
}

// Connect 建立连接并启动读循环。重复启动返回 ErrAlreadyStarted。
func (s *Session) Connect(ctx context.Context) error {
	s.mu.Lock()
	switch s.state {
	case StateClosed:
		s.mu.Unlock()
		return ErrClosed
	case StateConnecting, StateConnected, StateDisconnected:
		s.mu.Unlock()
		return ErrAlreadyStarted
	}
	err := s.connectLocked(ctx)
	s.mu.Unlock()
	return err
}

// Reconnect 仅允许在 StateDisconnected 时调用，其余状态返回可区分错误。
// 该方法会等待旧读循环完全退出后再拨号，保证同一时刻至多一个读 goroutine。
func (s *Session) Reconnect(ctx context.Context) error {
	s.mu.Lock()
	switch s.state {
	case StateClosed:
		s.mu.Unlock()
		return ErrClosed
	case StateDisconnected:
		// 唯一允许重连的状态。
	default:
		s.mu.Unlock()
		return ErrReconnectState
	}
	oldConn := s.conn
	oldDone := s.readDone
	if oldConn != nil {
		_ = oldConn.Close() // 促使旧读循环退出
	}
	s.mu.Unlock()

	if oldDone != nil {
		<-oldDone
	}

	s.mu.Lock()
	// 等待期间可能已被 Close。
	if s.state == StateClosed {
		s.mu.Unlock()
		return ErrClosed
	}
	err := s.connectLocked(ctx)
	s.mu.Unlock()
	return err
}

// readLoop 是连接的唯一读取 goroutine。任何异常都转化为状态迁移，
// panic 被 recover，保证不崩溃、不卡死。
func (s *Session) readLoop(conn Conn, done chan struct{}) {
	defer close(done)
	defer func() {
		if r := recover(); r != nil {
			s.emit(Event{Kind: EvReadError, Detail: "panic recovered"})
		}
	}()

	decoder := frame.NewDecoder(func(f *frame.Frame, bad *frame.ParseError) {
		if bad != nil {
			s.emit(Event{Kind: EvBadFrame, Bad: bad})
			return
		}
		s.emit(Event{Kind: EvGoodFrame, Frame: f})
	})

	buf := make([]byte, 2048)
	idle := s.opts.ReadIdle > 0
	stalled := 0
	for {
		if idle {
			_ = conn.SetReadDeadline(time.Now().Add(s.opts.ReadIdle))
		}
		n, err := conn.Read(buf)
		if n > 0 {
			stalled = 0
			decoder.Feed(buf[:n])
		}
		if err != nil {
			if isTimeout(err) {
				if idle {
					stalled++
					s.emit(Event{Kind: EvIdleTimeout, Detail: "no data within read idle deadline"})
					// 连续空闲超时由上层依据计数决定隔离；读循环本身继续存活，
					// 直到连接被 Close 或真正出错。
					continue
				}
				continue
			}
			s.handleReadExit(conn, err)
			return
		}
	}
}

// handleReadExit 将连接读失败转化为 Disconnected 状态。
// 若会话已被 Close 或连接已被替换（重连竞争），则不改动状态。
func (s *Session) handleReadExit(conn Conn, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state == StateClosed {
		return
	}
	if s.conn != conn {
		return
	}
	s.emit(Event{Kind: EvReadError, Detail: err.Error()})
	_ = conn.Close()
	s.conn = nil
	s.setStateLocked(StateDisconnected)
}

func isTimeout(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

// Close 永久关闭会话；可重入，重复关闭返回 ErrClosed。
// 返回 nil 表示本次调用完成了关闭（旧读循环已退出）。
func (s *Session) Close() error {
	s.mu.Lock()
	if s.state == StateClosed {
		s.mu.Unlock()
		return ErrClosed
	}
	conn := s.conn
	done := s.readDone
	s.setStateLocked(StateClosed)
	s.conn = nil
	s.connectedAt = time.Time{}
	s.mu.Unlock()

	if conn != nil {
		_ = conn.Close()
	}
	if done != nil {
		<-done
	}
	return nil
}

// Write 在已连接状态下发数据；未连接/已关闭返回可区分错误。
func (s *Session) Write(ctx context.Context, p []byte) error {
	s.mu.Lock()
	switch s.state {
	case StateClosed:
		s.mu.Unlock()
		return ErrClosed
	case StateConnected:
	default:
		s.mu.Unlock()
		return ErrNotConnected
	}
	conn := s.conn
	d := s.opts.WriteTimeout
	s.mu.Unlock()

	if d > 0 {
		_ = conn.SetWriteDeadline(time.Now().Add(d))
	}
	if _, err := conn.Write(p); err != nil {
		// 写失败意味着链路不可信：触发断开迁移。
		if ctx.Err() != nil {
			return ctx.Err()
		}
		s.handleReadExit(conn, err)
		return errors.Join(ErrWriteFailed, err)
	}
	return nil
}

// State 返回当前状态。
func (s *Session) State() State {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state
}

// SinceConnected 返回本次连接已建立时长；未连接返回 0。
func (s *Session) SinceConnected() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state != StateConnected || s.conn == nil {
		return 0
	}
	return time.Since(s.connectedAt)
}
