// Package sensor 定义统一的传感器读数模型、取值域校验与数据可用性判定。
//
// 判定语义：
//   - 越界（value 超出 Spec 的 [Min,Max]）：标记 VerdictOutOfRange 并拒绝；
//   - 过期（RecvTS-DeviceTS 超过 Spec.MaxAge；或设备时间戳超前）：
//     标记 VerdictStale 并拒绝；
//   - 乱序（Spec 不允许重排，且 DeviceTS 小于上一条已接受读数）：
//     标记 VerdictOutOfOrder 并拒绝；
//   - 其余读数 VerdictOK 并接受。
//
// Validator 是有状态的；连接重建或设备从隔离恢复后必须调用 Reset，
// 禁止沿用故障前的时间戳基线。
package sensor

import (
	"fmt"
	"time"
)

// Kind 标识传感器种类。
type Kind string

const (
	KindTemperature Kind = "temperature"
	KindHumidity    Kind = "humidity"
	KindPressure    Kind = "pressure"
)

// Reading 是统一归一化后的传感器读数。
type Reading struct {
	DeviceID string
	Kind     Kind
	Value    float64
	// Seq 为设备帧序号；DeviceTS 为设备时间戳(ms)；RecvTS 为本地接收时间(ms)。
	Seq      uint16
	DeviceTS int64
	RecvTS   int64
	// Verdict 为该读数的判定结论；RejectReason 非空时读数不可用。
	Verdict      Verdict
	RejectReason string
}

// Verdict 是读数判定结论。
type Verdict int

const (
	VerdictOK Verdict = iota
	VerdictOutOfRange
	VerdictStale
	VerdictOutOfOrder
)

func (v Verdict) String() string {
	switch v {
	case VerdictOK:
		return "ok"
	case VerdictOutOfRange:
		return "out_of_range"
	case VerdictStale:
		return "stale"
	case VerdictOutOfOrder:
		return "out_of_order"
	default:
		return fmt.Sprintf("verdict(%d)", int(v))
	}
}

// Spec 描述一类传感器的取值域与时效性约束。
type Spec struct {
	Kind         Kind
	Min          float64
	Max          float64
	MaxAge       time.Duration // 相对接收时间允许的最大设备时间戳偏差
	AllowReorder bool          // 是否允许设备时间戳乱序
}

// Validate 按取值域校验读数，返回判定与原因。
func (sp Spec) Validate(r Reading) (Verdict, string) {
	if r.Value < sp.Min || r.Value > sp.Max {
		return VerdictOutOfRange,
			fmt.Sprintf("value %.4f outside [%.4f,%.4f]", r.Value, sp.Min, sp.Max)
	}
	return VerdictOK, ""
}

// Validator 维护每设备的有状态判定：记录上一个已接受读数的设备时间戳，
// 以识别乱序与过期读数；连接重建后应调用 Reset，禁止沿用故障前状态。
type Validator struct {
	spec    Spec
	hasLast bool
	lastTS  int64
}

// NewValidator 创建判定器。
func NewValidator(sp Spec) *Validator { return &Validator{spec: sp} }

// Judge 判定一条读数，回填 Verdict/RejectReason。
// 返回的第二个值表示读数是否被接受（可供上层消费）。
// 被拒读数不会推进时间戳基线，避免一条坏数据污染后续判定。
func (v *Validator) Judge(r Reading) (Reading, bool) {
	// 1. 取值域。
	if verdict, reason := v.spec.Validate(r); verdict != VerdictOK {
		r.Verdict, r.RejectReason = verdict, reason
		return r, false
	}

	// 2. 时效性：设备时间戳过旧或超前过多都视为过期/不可信。
	if v.spec.MaxAge > 0 {
		maxAgeMs := v.spec.MaxAge.Milliseconds()
		if age := r.RecvTS - r.DeviceTS; age > maxAgeMs {
			r.Verdict = VerdictStale
			r.RejectReason = fmt.Sprintf("reading age %dms exceeds max %dms", age, maxAgeMs)
			return r, false
		} else if -age > maxAgeMs {
			r.Verdict = VerdictStale
			r.RejectReason = fmt.Sprintf("device timestamp %dms ahead of receive clock", -age)
			return r, false
		}
	}

	// 3. 乱序（按设备时间戳严格单调不下降）。
	if v.hasLast && !v.spec.AllowReorder && r.DeviceTS < v.lastTS {
		r.Verdict = VerdictOutOfOrder
		r.RejectReason = fmt.Sprintf("device ts %d < last accepted ts %d", r.DeviceTS, v.lastTS)
		return r, false
	}

	// 接受并推进基线。
	r.Verdict = VerdictOK
	v.hasLast = true
	v.lastTS = r.DeviceTS
	return r, true
}

// Reset 清空故障前的判定状态（用于重连/隔离恢复后）。
func (v *Validator) Reset() { v.hasLast, v.lastTS = false, 0 }

// IsUsable 判断读数在 nowMs 时刻是否可用：判定通过且未老化过期。
func IsUsable(r Reading, nowMs int64, sp Spec) bool {
	if r.Verdict != VerdictOK || r.RejectReason != "" {
		return false
	}
	if sp.MaxAge > 0 && nowMs-r.DeviceTS > sp.MaxAge.Milliseconds() {
		return false
	}
	return true
}
