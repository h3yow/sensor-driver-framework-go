package sensor

import (
	"encoding/binary"
	"errors"
	"math"
)

// ErrBadPayload 表示读数帧负载无法解码。
var ErrBadPayload = errors.New("sensor: bad reading payload")

// EncodeReadingPayload 将一个 double 浮点读数编码为读数帧负载（8 字节小端）。
func EncodeReadingPayload(v float64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, math.Float64bits(v))
	return b
}

// DecodeReadingPayload 解码读数帧负载。
func DecodeReadingPayload(b []byte) (float64, error) {
	if len(b) != 8 {
		return 0, ErrBadPayload
	}
	return math.Float64frombits(binary.LittleEndian.Uint64(b)), nil
}
