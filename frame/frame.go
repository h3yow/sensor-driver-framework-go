// Package frame 定义传感器传输层的二进制分帧格式与流式容错解析器。
//
// 帧格式（小端序）：
//
//	+--------+------+--------+----------+------------+-----------------+-------+
//	| magic  | type | seq    | ts       | payloadLen | payload         | crc16 |
//	| 2 byte | 1 B  | 2 byte | 8 byte   | 2 byte     | payloadLen 字节 | 2 B   |
//	+--------+------+--------+----------+------------+-----------------+-------+
//
// magic=0xA55A（线上字节序为 0x5A,0xA5）；crc16 覆盖从 type 到 payload
// 末尾（不含 magic 与 crc 自身），算法为 CRC-16/MODBUS。
//
// 解析容错规则：
//   - 半包（帧头/帧体未收全）：保留已收字节等待后续 Feed，不产生坏帧事件；
//   - 粘包：一次 Feed 中连续吐出多帧；
//   - 坏 magic：逐字节重新同步到下一个 magic，被跳过字节以 bad_magic 上报；
//   - 声明长度越界、CRC 校验失败：丢弃当前帧并从下一 magic 重新同步，
//     坏帧不会污染后续帧；每一次坏帧与重同步都通过回调可观察。
package frame

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// Magic 标识帧起始。
const Magic uint16 = 0xA55A

// 固定部分长度：magic(2)+type(1)+seq(2)+ts(8)+len(2)+crc(2)。
const HeaderSize = 17

// MaxPayloadLen 是单帧负载上限。
const MaxPayloadLen = 512

// Type 标识帧的负载类型。
type Type uint8

const (
	TypeReading Type = 0x01 // 传感器读数帧
	TypeAck     Type = 0x02 // 命令应答帧
	TypeEvent   Type = 0x03 // 设备事件帧
)

// Frame 是一帧解析后的结构化数据。Payload 为解析器内部缓冲的拷贝，
// 调用方可以长期持有。
type Frame struct {
	Type    Type
	Seq     uint16
	TS      int64 // 设备时间戳（unix 毫秒）
	Payload []byte
}

// ParseError 描述一次坏帧事件，供上层做可观察的恢复统计。
type ParseError struct {
	Kind    string
	Detail  string
	RawByte byte
}

func (e *ParseError) Error() string {
	return fmt.Sprintf("frame: %s (%s)", e.Kind, e.Detail)
}

// 解析器可能产生的错误类别（用于 ParseError.Kind）。
const (
	ErrBadMagic     = "bad_magic"
	ErrLengthTooBig = "length_too_big"
	ErrCRC          = "crc_mismatch"
)

// ErrShortBuffer 表示缓冲区内连帧头都不完整，属于正常的半包等待，不是坏帧。
var ErrShortBuffer = errors.New("frame: short buffer")

// EventHandler 接收解析器吐出的好帧与坏帧事件。坏帧时 f 为 nil、bad 非空。
type EventHandler func(f *Frame, bad *ParseError)

// Decoder 是有状态的流式解析器：持续向其 Feed 字节流，
// 半包自动等待、粘包一次吐出多帧、坏帧跳过并报告。
type Decoder struct {
	buf     []byte
	handler EventHandler
	stats   Stats
}

// NewDecoder 创建解析器。handler 不得为 nil。
func NewDecoder(handler EventHandler) *Decoder {
	if handler == nil {
		panic("frame: nil handler")
	}
	return &Decoder{handler: handler}
}

// Feed 输入新到达的字节（可为空），解析结果通过 handler 回调。
// 半包字节保留在解析器内部；返回累计的解析统计。
func (d *Decoder) Feed(b []byte) Stats {
	d.buf = append(d.buf, b...)
	d.parse()
	return d.stats
}

// Pending 返回内部缓存中尚未成帧的字节数（半包观察用）。
func (d *Decoder) Pending() int { return len(d.buf) }

func (d *Decoder) emitBad(kind, detail string, raw byte) {
	d.stats.BadFrames++
	d.stats.Resyncs++
	d.handler(nil, &ParseError{Kind: kind, Detail: detail, RawByte: raw})
}

// parse 尽可能多地从 d.buf 中解析帧，结束时 d.buf 只保留：
//   - 空；或
//   - 一个半包（magic 正确但帧体未收全）。
func (d *Decoder) parse() {
	for len(d.buf) > 0 {
		// 1. 定位帧头 magic。
		if len(d.buf) < 2 {
			return // 连 magic 都凑不齐，等待更多字节
		}
		if binary.LittleEndian.Uint16(d.buf[0:2]) != Magic {
			// 坏 magic：报告当前字节并跳过 1 字节，继续寻找下一 magic。
			bad := d.buf[0]
			d.emitBad(ErrBadMagic, fmt.Sprintf("unexpected byte 0x%02x", bad), bad)
			d.buf = d.buf[1:]
			continue
		}

		// 2. magic 正确，尝试读取帧头。
		if len(d.buf) < HeaderSize {
			return // 半包：帧头未收全，原样保留
		}
		payloadLen := int(binary.LittleEndian.Uint16(d.buf[13:15]))
		if payloadLen > MaxPayloadLen {
			// 声明长度非法：当前帧不可信，且无法确定其边界。
			// 一次性扫描到下一个 magic 处重新同步，只产生一个坏帧事件，
			// 避免对残余字节逐个报噪音，也保证后续帧不被污染。
			d.emitBad(ErrLengthTooBig,
				fmt.Sprintf("payloadLen=%d > max=%d", payloadLen, MaxPayloadLen), d.buf[0])
			if idx := findMagic(d.buf[2:]); idx >= 0 {
				d.buf = d.buf[2+idx:]
			} else {
				// 缓冲区内没有下一个 magic：损坏帧长度未知，全部丢弃。
				d.buf = nil
			}
			continue
		}

		total := HeaderSize + payloadLen
		if len(d.buf) < total {
			return // 半包：帧体未收全，原样保留
		}

		// 3. 整帧到齐，校验 CRC（覆盖 type..payload，即 [2:total-2]）。
		body := d.buf[2 : total-2]
		gotCRC := binary.LittleEndian.Uint16(d.buf[total-2 : total])
		if calc := CRC16(body); calc != gotCRC {
			d.emitBad(ErrCRC,
				fmt.Sprintf("want=0x%04x got=0x%04x", calc, gotCRC), d.buf[0])
			// 跳过当前帧全部字节，从下一 magic 重新同步。
			d.buf = d.buf[total:]
			continue
		}

		// 4. 好帧：拷贝 payload 后回调，避免与内部缓冲别名。
		f := &Frame{
			Type:    Type(d.buf[2]),
			Seq:     binary.LittleEndian.Uint16(d.buf[3:5]),
			TS:      int64(binary.LittleEndian.Uint64(d.buf[5:13])),
			Payload: append([]byte(nil), d.buf[15:15+payloadLen]...),
		}
		d.stats.GoodFrames++
		d.handler(f, nil)
		d.buf = d.buf[total:]
	}
}

// Stats 是解析统计（累计值）。
type Stats struct {
	GoodFrames int
	BadFrames  int
	Resyncs    int
}

// findMagic 返回 b 中帧头 magic 的起始下标，未找到返回 -1。
// 若仅最后一个字节匹配 magic 首字节，暂不认定（可能是半包），返回 -1。
func findMagic(b []byte) int {
	for i := 0; i+1 < len(b); i++ {
		if binary.LittleEndian.Uint16(b[i:i+2]) == Magic {
			return i
		}
	}
	return -1
}

// Encode 将一帧编码为完整字节（含 CRC）。未知类型/超长负载返回错误。
func Encode(f *Frame) ([]byte, error) {
	if f == nil {
		return nil, errors.New("frame: nil frame")
	}
	if f.Type != TypeReading && f.Type != TypeAck && f.Type != TypeEvent {
		return nil, fmt.Errorf("frame: unknown type 0x%02x", f.Type)
	}
	if len(f.Payload) > MaxPayloadLen {
		return nil, fmt.Errorf("frame: payload too long %d > %d", len(f.Payload), MaxPayloadLen)
	}
	out := make([]byte, HeaderSize+len(f.Payload))
	binary.LittleEndian.PutUint16(out[0:2], Magic)
	out[2] = byte(f.Type)
	binary.LittleEndian.PutUint16(out[3:5], f.Seq)
	binary.LittleEndian.PutUint64(out[5:13], uint64(f.TS))
	binary.LittleEndian.PutUint16(out[13:15], uint16(len(f.Payload)))
	copy(out[15:15+len(f.Payload)], f.Payload)
	crc := CRC16(out[2 : 15+len(f.Payload)])
	binary.LittleEndian.PutUint16(out[15+len(f.Payload):], crc)
	return out, nil
}

// CRC16 使用 CRC-16/MODBUS（多项式 0xA001，初值 0xFFFF）计算校验值。
func CRC16(b []byte) uint16 {
	crc := uint16(0xFFFF)
	for _, x := range b {
		crc ^= uint16(x)
		for i := 0; i < 8; i++ {
			if crc&1 != 0 {
				crc = (crc >> 1) ^ 0xA001
			} else {
				crc >>= 1
			}
		}
	}
	return crc
}
