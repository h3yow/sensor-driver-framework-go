// Package simdev 提供可编程的模拟传感器设备端，用于替代真实硬件。
//
// 设备在本地回环地址监听 TCP，接受连接后按 Behavior 脚本发送帧，
// 可注入半包、粘包、坏帧、阻塞与时间戳乱序，行为完全确定、可复现。
package simdev

import (
	"encoding/binary"
	"log"
	"math"
	"net"
	"sync"
	"time"

	"github.com/h3yow/sensor-driver-framework-go/pkg/frame"
)

// Behavior 描述设备端一段可编程行为（零值表示理想设备）。
type Behavior struct {
	SplitFrames   bool // 将每帧拆成两段发送（制造半包）
	Sticky        bool // 将多帧合并为一次发送（制造粘包）
	CorruptEvery  int  // 每 N 帧注入一个 CRC 坏帧（0 表示不注入）
	BlockAfter    int  // 每发送 N 帧后阻塞 BlockMs 毫秒（0 表示不阻塞）
	BlockMs       int
	ReorderWindow int // 在窗口内反转发送顺序（制造时间戳乱序，0/1 表示顺序）
	TsOffsetMs    int // 时间戳偏移（负值制造过期读数）
	IntervalMs    int // 帧间隔（默认 10ms）
}

// PayloadLen 是读数负载长度：ts(8, unixnano BE) + value(8, float64 BE)。
const PayloadLen = 16

// EncodePayload 编码一条读数负载。
func EncodePayload(ts time.Time, value float64) []byte {
	p := make([]byte, PayloadLen)
	binary.BigEndian.PutUint64(p[0:8], uint64(ts.UnixNano()))
	binary.BigEndian.PutUint64(p[8:16], math.Float64bits(value))
	return p
}

// DecodePayload 解码一条读数负载。
func DecodePayload(p []byte) (ts time.Time, value float64, ok bool) {
	if len(p) != PayloadLen {
		return time.Time{}, 0, false
	}
	ts = time.Unix(0, int64(binary.BigEndian.Uint64(p[0:8])))
	value = math.Float64frombits(binary.BigEndian.Uint64(p[8:16]))
	return ts, value, true
}

// Device 是一个基于 TCP 的模拟传感器设备。
type Device struct {
	Addr string
	ln   net.Listener
	done chan struct{}
	once sync.Once
	wg   sync.WaitGroup
}

// New 在本地回环地址启动一个模拟设备，按脚本行为发送 frames 帧
// （frames<=0 表示持续发送直到关闭）。
func New(name string, b Behavior, frames int) (*Device, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	d := &Device{Addr: ln.Addr().String(), ln: ln, done: make(chan struct{})}
	d.wg.Add(1)
	go d.acceptLoop(name, b, frames)
	log.Printf("[simdev] device %s listening at %s behavior=%+v", name, d.Addr, b)
	return d, nil
}

// Close 停止设备并等待所有连接处理结束。
func (d *Device) Close() {
	d.once.Do(func() {
		close(d.done)
		_ = d.ln.Close()
	})
	d.wg.Wait()
}

func (d *Device) acceptLoop(name string, b Behavior, frames int) {
	defer d.wg.Done()
	for {
		conn, err := d.ln.Accept()
		if err != nil {
			return // 监听关闭
		}
		d.wg.Add(1)
		go func() {
			defer d.wg.Done()
			defer conn.Close()
			d.serve(name, conn, b, frames)
		}()
	}
}

// serve 按脚本向一条连接发送帧流。
func (d *Device) serve(name string, conn net.Conn, b Behavior, frames int) {
	interval := time.Duration(b.IntervalMs) * time.Millisecond
	if interval <= 0 {
		interval = 10 * time.Millisecond
	}
	sent := 0
	var window []frame.Frame // 乱序窗口缓冲
	var stickyBuf []byte     // 粘包缓冲
	sentInBlock := 0

	maybeBlock := func() bool {
		if b.BlockAfter > 0 && b.BlockMs > 0 {
			sentInBlock++
			if sentInBlock >= b.BlockAfter {
				sentInBlock = 0
				log.Printf("[simdev] device %s blocking for %dms after %d frames", name, b.BlockMs, sent)
				select {
				case <-d.done:
					return false
				case <-time.After(time.Duration(b.BlockMs) * time.Millisecond):
				}
			}
		}
		return true
	}

	for frames <= 0 || sent < frames {
		select {
		case <-d.done:
			return
		default:
		}
		seq := uint16(sent)
		ts := time.Now().Add(time.Duration(b.TsOffsetMs) * time.Millisecond)
		value := 20.0 + float64(sent%10) // 确定性的模拟读数
		f := frame.Frame{Type: 0x01, Seq: seq, Payload: EncodePayload(ts, value)}
		sent++

		// 乱序：窗口攒满后反转发送
		if b.ReorderWindow > 1 {
			window = append(window, f)
			if len(window) < b.ReorderWindow {
				continue
			}
			var buf []byte
			for i := len(window) - 1; i >= 0; i-- {
				buf = append(buf, encode(name, window[i], b, sent)...)
			}
			window = window[:0]
			if !d.writeAll(conn, buf, b) || !maybeBlock() {
				return
			}
			time.Sleep(interval)
			continue
		}

		raw := encode(name, f, b, sent)
		if b.Sticky {
			// 粘包：攒够 3 帧（或达到帧数上限）后一次写出
			stickyBuf = append(stickyBuf, raw...)
			if sent%3 != 0 && (frames <= 0 || sent < frames) {
				continue
			}
			raw = stickyBuf
			stickyBuf = nil
		}
		if !d.writeAll(conn, raw, b) || !maybeBlock() {
			return
		}
		time.Sleep(interval)
	}
	// 帧发完后保持连接，直到设备关闭（模拟静默设备）。
	<-d.done
}

// encode 按行为编码单帧：可能破坏 CRC。
func encode(name string, f frame.Frame, b Behavior, sent int) []byte {
	raw := frame.Encode(f)
	if b.CorruptEvery > 0 && sent%b.CorruptEvery == 0 {
		raw[len(raw)-1] ^= 0xFF // 破坏 CRC
		log.Printf("[simdev] device %s injecting corrupt frame seq=%d", name, f.Seq)
	}
	return raw
}

// writeAll 按行为写出字节流：半包拆分或整体写出。
func (d *Device) writeAll(conn net.Conn, buf []byte, b Behavior) bool {
	if len(buf) == 0 {
		return true
	}
	if b.SplitFrames {
		// 半包：分两段写，中间留小间隔
		half := len(buf) / 2
		if half > 0 {
			if _, err := conn.Write(buf[:half]); err != nil {
				return false
			}
			select {
			case <-d.done:
				return false
			case <-time.After(2 * time.Millisecond):
			}
			buf = buf[half:]
		}
	}
	_, err := conn.Write(buf)
	return err == nil
}
