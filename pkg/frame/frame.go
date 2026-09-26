// Package frame 实现传感器传输层的分帧与容错解析。
//
// 帧布局: [MAGIC(2)=0xAA55] [TYPE(1)] [SEQ(2,BE)] [LEN(2,BE)] [PAYLOAD(N)] [CRC16(2,BE)]
// CRC16 覆盖 TYPE..PAYLOAD，采用 CCITT-FALSE。
package frame

import (
	"encoding/binary"
	"log"
)

const (
	Magic0     = 0xAA
	Magic1     = 0x55
	HeaderLen  = 7 // magic(2)+type(1)+seq(2)+len(2)
	CrcLen     = 2
	MaxPayload = 1024
)

// Frame 表示一个解析完成的应用层帧。
type Frame struct {
	Type    byte
	Seq     uint16
	Payload []byte
}

// Encode 将帧编码为字节流（含 CRC）。
func Encode(f Frame) []byte {
	n := HeaderLen + len(f.Payload) + CrcLen
	buf := make([]byte, n)
	buf[0] = Magic0
	buf[1] = Magic1
	buf[2] = f.Type
	binary.BigEndian.PutUint16(buf[3:5], f.Seq)
	binary.BigEndian.PutUint16(buf[5:7], uint16(len(f.Payload)))
	copy(buf[HeaderLen:], f.Payload)
	crc := CRC16(buf[2 : HeaderLen+len(f.Payload)])
	binary.BigEndian.PutUint16(buf[n-CrcLen:], crc)
	return buf
}

// CRC16 计算 CCITT-FALSE 校验（多项式 0x1021，初值 0xFFFF）。
func CRC16(data []byte) uint16 {
	crc := uint16(0xFFFF)
	for _, b := range data {
		crc ^= uint16(b) << 8
		for i := 0; i < 8; i++ {
			if crc&0x8000 != 0 {
				crc = (crc << 1) ^ 0x1021
			} else {
				crc <<= 1
			}
		}
	}
	return crc
}

// Stats 记录解析器的容错统计。
type Stats struct {
	FramesOK     int
	BadCRC       int
	Resyncs      int
	BytesDropped int
}

// Decoder 是流式分帧器，支持半包、粘包与坏帧重同步。
// 坏帧只丢弃到下一个 magic 之前的字节，不污染后续帧。
type Decoder struct {
	buf        []byte
	stats      Stats
	OnRecovery func(s Stats) // 连续异常达到阈值时触发
	badStreak  int
	threshold  int
}

// NewDecoder 创建分帧器；recoveryThreshold 为连续坏帧触发恢复动作的阈值（<=0 表示 3）。
func NewDecoder(recoveryThreshold int) *Decoder {
	if recoveryThreshold <= 0 {
		recoveryThreshold = 3
	}
	return &Decoder{threshold: recoveryThreshold}
}

// Feed 喂入字节流，返回本次解析出的完整帧。
func (d *Decoder) Feed(chunk []byte) []Frame {
	d.buf = append(d.buf, chunk...)
	var out []Frame
	for {
		f, ok := d.tryParse()
		if !ok {
			break
		}
		if f != nil {
			out = append(out, *f)
		}
	}
	return out
}

// tryParse 尝试从缓冲区解析一帧。
// 返回 (nil,false) 表示数据不足（半包，等待更多数据）。
// 返回 (nil,true) 表示丢弃了坏数据，可继续尝试。
func (d *Decoder) tryParse() (*Frame, bool) {
	// 重同步：丢弃 magic 之前的垃圾字节。
	for len(d.buf) >= 2 && !(d.buf[0] == Magic0 && d.buf[1] == Magic1) {
		d.buf = d.buf[1:]
		d.stats.BytesDropped++
		d.stats.Resyncs++
	}
	if len(d.buf) < HeaderLen {
		return nil, false // 半包：头部不完整
	}
	payloadLen := int(binary.BigEndian.Uint16(d.buf[5:7]))
	if payloadLen > MaxPayload {
		// 长度字段非法：判定为坏帧，跳过一个 magic 字节后重同步。
		d.dropBad("invalid length")
		return nil, true
	}
	total := HeaderLen + payloadLen + CrcLen
	if len(d.buf) < total {
		return nil, false // 半包：负载不完整
	}
	raw := d.buf[:total]
	d.buf = d.buf[total:]
	want := binary.BigEndian.Uint16(raw[total-CrcLen:])
	got := CRC16(raw[2 : total-CrcLen])
	if want != got {
		d.stats.BadCRC++
		d.stats.BytesDropped += total
		d.onBad("crc mismatch")
		return nil, true
	}
	d.badStreak = 0
	d.stats.FramesOK++
	f := &Frame{
		Type:    raw[2],
		Seq:     binary.BigEndian.Uint16(raw[3:5]),
		Payload: append([]byte(nil), raw[HeaderLen:total-CrcLen]...),
	}
	return f, true
}

// dropBad 丢弃当前 magic 首字节并记录，使下一轮重同步到下一个候选 magic。
func (d *Decoder) dropBad(reason string) {
	d.buf = d.buf[1:]
	d.stats.BytesDropped++
	d.stats.Resyncs++
	d.onBad(reason)
}

func (d *Decoder) onBad(reason string) {
	d.badStreak++
	log.Printf("[frame] bad frame dropped: reason=%s streak=%d stats=%+v", reason, d.badStreak, d.stats)
	if d.badStreak >= d.threshold {
		log.Printf("[frame] recovery action: bad streak %d >= threshold %d, resyncing stream", d.badStreak, d.threshold)
		if d.OnRecovery != nil {
			d.OnRecovery(d.stats)
		}
		d.badStreak = 0
	}
}

// Stats 返回当前统计快照。
func (d *Decoder) Stats() Stats { return d.stats }
