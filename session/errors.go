package session

import "errors"

// 可区分的会话失败原因。上层可通过 errors.Is 精确区分拒绝原因。
var (
	// ErrAlreadyStarted 重复启动（Connect 已调用过，或会话仍在运行）。
	ErrAlreadyStarted = errors.New("session: already started")
	// ErrNotConnected 在未处于 Connected 状态时执行读写。
	ErrNotConnected = errors.New("session: not connected")
	// ErrClosed 会话已永久关闭；关闭后不允许再启动或重连。
	ErrClosed = errors.New("session: closed")
	// ErrDialFailed 建立底层连接失败。
	ErrDialFailed = errors.New("session: dial failed")
	// ErrReconnectState 仅 Disconnected 状态允许重连。
	ErrReconnectState = errors.New("session: reconnect only allowed in disconnected state")
	// ErrWriteFailed 已连接状态下写出错（连接同时转入 Disconnected）。
	ErrWriteFailed = errors.New("session: write failed")
	// ErrNilDialer 构造时未提供拨号函数。
	ErrNilDialer = errors.New("session: nil dialer")
)
