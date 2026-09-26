// Package sim 提供可编程的模拟设备端，在本机内存里模拟真实传感器链路，
// 支持半包、粘包、坏帧、乱序时间戳、阻塞与主动断链等脚本化行为。
//
// 设备不监听任何真实端口：Dialer 内部通过 net.Pipe 建立成对内存连接，
// 拨号一次即“接受一次连接”，并在独立 goroutine 中按 Script 执行行为；
// Behavior 可依据连接尝试序号返回不同脚本（用于模拟故障后恢复）。
package sim

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/h3yow/sensor-driver-framework-go/frame"
	"github.com/h3yow/sensor-driver-framework-go/session"
)

// StepKind 是脚本动作类型。
type StepKind int

const (
	StepSendFrame StepKind = iota // 编码发送一帧（可选半包拆分）
	StepSendRaw                   // 原样写字节（用于制造坏帧/垃圾字节）
	StepSleep                     // 等待一段时间
	StepBlock                     // 静默阻塞，模拟设备无响应，直到对端关闭
	StepClose                     // 主动断开连接
)

// Step 是模拟脚本中的一个动作。
type Step struct {
	Kind StepKind
	// Frame 为发送帧内容（Kind=StepSendFrame 时使用）。
	Frame frame.Frame
	// Raw 为要原样写入的原始字节（StepSendRaw 时使用，用于制造坏帧）。
	Raw []byte
	// Split 为 true 时先写一半数据，等待 SplitDelay 后再写另一半，制造半包。
	Split      bool
	SplitDelay time.Duration
	// PackWithNext 为 true 时，本动作的数据暂不写出，与下一动作的数据
	// 在同一次 Write 中发出，制造粘包。
	PackWithNext bool
	// Sleep 为 StepSleep 的等待时长。
	Sleep time.Duration
}

// Script 是一次连接生命周期内的模拟行为。
type Script struct {
	Steps []Step
}

// Behavior 按连接尝试序号返回该次连接使用的脚本（首次为 attempt=1）。
type Behavior func(attempt int) Script

// Device 是一个内存模拟设备。
type Device struct {
	behavior Behavior

	mu       sync.Mutex
	attempts int
	upNanos  int64
}

// New 创建模拟设备，不监听任何真实端口。
// behavior 为 nil 时返回空脚本（连上后静默，等待对端操作）。
func New(behavior Behavior) *Device {
	if behavior == nil {
		behavior = func(int) Script { return Script{} }
	}
	return &Device{behavior: behavior}
}

type pipeConn struct{ net.Conn }

// Dialer 返回适配 session.Dialer 的拨号函数；每次拨号建立一条内存链路
// 并运行一次脚本。拨号序号在 Close 后再次 Dial 时递增。
func (d *Device) Dialer() session.Dialer {
	return func(ctx context.Context) (session.Conn, error) {
		client, server := net.Pipe()

		d.mu.Lock()
		d.attempts++
		attempt := d.attempts
		d.mu.Unlock()

		script := d.behavior(attempt)
		go d.serve(server, script, attempt)

		select {
		case <-ctx.Done():
			_ = client.Close()
			_ = server.Close()
			return nil, ctx.Err()
		default:
		}
		return client, nil
	}
}

func (d *Device) serve(conn net.Conn, script Script, attempt int) {
	defer conn.Close()
	start := time.Now()
	defer func() { atomic.AddInt64(&d.upNanos, time.Since(start).Nanoseconds()) }()

	// pending 收集 PackWithNext 的粘包数据，在下一次写出时合并。
	var pending []byte
	flush := func(data []byte, split bool, delay time.Duration) bool {
		data = append(append([]byte(nil), pending...), data...)
		pending = nil
		if len(data) == 0 {
			return true
		}
		if !split || len(data) < 2 {
			return writeAll(conn, data)
		}
		half := len(data) / 2
		if !writeAll(conn, data[:half]) {
			return false
		}
		if !waitOrClose(conn, delay) {
			return false
		}
		return writeAll(conn, data[half:])
	}

	for i := range script.Steps {
		st := script.Steps[i]
		switch st.Kind {
		case StepSendFrame:
			b, err := frame.Encode(&st.Frame)
			if err != nil {
				// 脚本本身非法：直接结束本次连接，错误不应 panic。
				return
			}
			if st.PackWithNext {
				pending = append(pending, b...)
				continue
			}
			if !flush(b, st.Split, st.SplitDelay) {
				return
			}
		case StepSendRaw:
			if st.PackWithNext {
				pending = append(pending, st.Raw...)
				continue
			}
			if !flush(st.Raw, st.Split, st.SplitDelay) {
				return
			}
		case StepSleep:
			if !waitOrClose(conn, st.Sleep) {
				return
			}
		case StepBlock:
			// 静默不写：依赖连接关闭检测退出（对端 SetReadDeadline
			// 超时不会关闭连接，这里持续等待直到 Close）。
			if !waitOrClose(conn, 0) {
				return
			}
		case StepClose:
			return
		}
	}

	// 脚本执行完毕后保持连接存活（等待对端关闭），避免 EOF 意外影响判定。
	_ = waitOrClose(conn, 0)
}

// writeAll 处理 net.Pipe 的短写，返回 false 表示连接已坏。
func writeAll(conn net.Conn, b []byte) bool {
	for len(b) > 0 {
		n, err := conn.Write(b)
		if err != nil {
			return false
		}
		b = b[n:]
	}
	return true
}

// waitOrClose 等待 d；d<=0 时无限等待。期间对端关闭连接会立即返回 false。
func waitOrClose(conn net.Conn, d time.Duration) bool {
	done := make(chan struct{})
	go func() {
		defer close(done)
		one := make([]byte, 1)
		_, _ = conn.Read(one)
	}()
	var timer <-chan time.Time
	if d > 0 {
		t := time.NewTimer(d)
		defer t.Stop()
		timer = t.C
	}
	select {
	case <-done:
		return false
	case <-timer:
		return true
	}
}

// Attempts 返回设备被拨号的总次数。
func (d *Device) Attempts() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.attempts
}

// Uptime 返回设备端各次连接累计存活时长（仅测试观察用）。
func (d *Device) Uptime() time.Duration {
	return time.Duration(atomic.LoadInt64(&d.upNanos))
}
