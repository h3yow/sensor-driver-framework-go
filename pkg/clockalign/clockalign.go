// Package clockalign 实现设备时钟对齐与时钟异常防护。
//
// 每台设备独立维护一份有界状态（固定大小，无历史缓冲）：
//   - 用 EWMA 估计设备时钟相对统一时间基准（采集端时钟）的偏差，
//     能跟上缓慢漂移，但明显偏离当前估计的样本会被拒绝，不会带偏估计；
//   - 对明显超前、明显滞后、完全不合理的时间戳给出可区分的判定；
//   - 连续异常达到阈值视为时钟跳变/重启（重新计时），自动重建时间序；
//   - 长时间无法对齐时上报 Unaligned，由采集层按策略降级。
//
// 判定语义：
//   - 未对齐阶段仅用硬边界（InvalidBeforeMs / HardFutureMs）过滤完全不合理样本，
//     其余样本进入学习，攒够 MinSamples 后建立对齐；
//   - 对齐后以“到达时刻 - 估计偏差”推算设备当前应有时刻，读数偏离该时刻
//     超过 MaxFutureMs 判为明显超前，超过 MaxLagMs 判为明显滞后；
//   - 异常样本不更新偏差估计，也不应成为后续读数的比较基准；
//   - 连续 ResetThreshold 条异常视为时钟跳变/重启：清空估计并就地重新学习，
//     若新纪元早于 InvalidBeforeMs（设备从零重新计时），下调本设备的硬下界。
package clockalign

import (
	"log"
	"time"
)

// Anomaly 是可区分的时钟异常判定原因。
type Anomaly int

const (
	AnomalyNone    Anomaly = iota // 无异常
	AnomalyFuture                 // 明显超前
	AnomalyBehind                 // 明显滞后
	AnomalyInvalid                // 完全不合理（超出硬边界）
	AnomalyJump                   // 时钟跳变/重新计时（已触发重建）
)

func (a Anomaly) String() string {
	switch a {
	case AnomalyNone:
		return "None"
	case AnomalyFuture:
		return "Future"
	case AnomalyBehind:
		return "Behind"
	case AnomalyInvalid:
		return "Invalid"
	case AnomalyJump:
		return "Jump"
	}
	return "Unknown"
}

// Config 是时钟对齐与异常防护参数（零值取默认）。
type Config struct {
	Enabled         bool    // 是否启用（默认 false，行为与未接入本层一致）
	MaxFutureMs     int     // 相对估计偏差超前阈值（默认 2000ms）
	MaxLagMs        int     // 相对估计偏差滞后阈值（默认 5000ms）
	InvalidBeforeMs int64   // 早于此 Unix 毫秒时刻视为完全不合理（默认 946684800000 = 2000-01-01）
	HardFutureMs    int     // 超过到达时间+该值视为完全不合理（默认 24h）
	ResetThreshold  int     // 连续异常达到该值触发时钟重建（默认 3）
	MinSamples      int     // 建立对齐所需最少有效样本（默认 3）
	Alpha           float64 // EWMA 平滑系数（默认 0.2）
	AlignTimeoutMs  int     // 超过该时长仍未对齐判定为无法对齐（默认 30000ms）
}

// WithDefaults 填充默认参数。
func (c Config) WithDefaults() Config {
	out := c
	if out.MaxFutureMs <= 0 {
		out.MaxFutureMs = 2000
	}
	if out.MaxLagMs <= 0 {
		out.MaxLagMs = 5000
	}
	if out.InvalidBeforeMs == 0 {
		out.InvalidBeforeMs = 946684800000 // 2000-01-01T00:00:00Z
	}
	if out.HardFutureMs <= 0 {
		out.HardFutureMs = 24 * 3600 * 1000
	}
	if out.ResetThreshold <= 0 {
		out.ResetThreshold = 3
	}
	if out.MinSamples <= 0 {
		out.MinSamples = 3
	}
	if out.Alpha <= 0 || out.Alpha > 1 {
		out.Alpha = 0.2
	}
	if out.AlignTimeoutMs <= 0 {
		out.AlignTimeoutMs = 30000
	}
	return out
}

// Result 是一条读数时间戳的对齐结果。
type Result struct {
	AlignedTS time.Time     // 统一时间轴上的时间（未对齐时等于原始时间）
	Adjusted  bool          // 本次是否按估计偏差调整过
	Anomaly   Anomaly       // 异常判定原因
	Reset     bool          // 本次是否触发了时钟状态重建
	Aligned   bool          // 处理后设备是否已对齐
	Unaligned bool          // 设备长时间无法对齐
	Offset    time.Duration // 当前偏差估计（aligned = raw + Offset）
}

// DeviceClock 是单设备时钟状态（固定大小，无历史样本缓冲，内存有界）。
type DeviceClock struct {
	cfg Config

	offset    time.Duration // EWMA 偏差估计：aligned = raw + offset
	samples   int           // 有效样本数（达到 MinSamples 后视为已对齐）
	aligned   bool
	consecBad int // 连续异常样本数

	invalidBefore time.Time // 本设备硬下界（纪元重置后可下调）
	sinceReset    time.Time // 上次重建时刻，用于 AlignTimeout 判定
	unaligned     bool      // 已上报过“长时间无法对齐”
}

// New 创建一台设备的时钟对齐状态。
func New(cfg Config) *DeviceClock {
	c := cfg.WithDefaults()
	return &DeviceClock{
		cfg:           c,
		invalidBefore: time.UnixMilli(c.InvalidBeforeMs),
		sinceReset:    time.Now(),
	}
}

// Reset 重置时钟状态（重连/重启后重新建立时间序）。
func (d *DeviceClock) Reset() {
	d.offset = 0
	d.samples = 0
	d.aligned = false
	d.consecBad = 0
	d.invalidBefore = time.UnixMilli(d.cfg.InvalidBeforeMs)
	d.sinceReset = time.Now()
	d.unaligned = false
	log.Printf("[clockalign] state reset (manual)")
}

// Offset 返回当前偏差估计；ok 为 false 表示尚未对齐。
func (d *DeviceClock) Offset() (time.Duration, bool) {
	return d.offset, d.aligned
}

// Align 对一条设备读数时间戳做异常判定与对齐；arrival 为采集端收到时刻。
func (d *DeviceClock) Align(rawTS, arrival time.Time) Result {
	res := Result{AlignedTS: rawTS}

	// 1. 完全不合理：超出硬边界（任何阶段都检查）。
	if rawTS.Before(d.invalidBefore) {
		res.Anomaly = AnomalyInvalid
	} else if rawTS.After(arrival.Add(time.Duration(d.cfg.HardFutureMs) * time.Millisecond)) {
		res.Anomaly = AnomalyInvalid
	} else if d.aligned {
		// 2. 已对齐：相对估计偏差的方向性异常。
		expected := arrival.Add(-d.offset) // 设备此刻应有的时钟读数
		err := rawTS.Sub(expected)
		switch {
		case err > time.Duration(d.cfg.MaxFutureMs)*time.Millisecond:
			res.Anomaly = AnomalyFuture
		case err < -time.Duration(d.cfg.MaxLagMs)*time.Millisecond:
			res.Anomaly = AnomalyBehind
		}
	}
	// 未对齐且通过硬边界：视为可学习样本，不做方向性判定。

	if res.Anomaly != AnomalyNone {
		return d.onAnomaly(rawTS, arrival, res)
	}

	// 3. 有效样本：更新 EWMA 偏差估计。
	d.consecBad = 0
	sample := arrival.Sub(rawTS)
	if d.samples == 0 {
		d.offset = sample
	} else {
		d.offset = time.Duration(float64(d.offset)*(1-d.cfg.Alpha) + float64(sample)*d.cfg.Alpha)
	}
	d.samples++
	if !d.aligned && d.samples >= d.cfg.MinSamples {
		d.aligned = true
		d.unaligned = false
		log.Printf("[clockalign] aligned after %d samples, offset=%v", d.samples, d.offset)
	}

	if d.aligned {
		res.AlignedTS = rawTS.Add(d.offset)
		res.Adjusted = d.offset != 0
		res.Offset = d.offset
		res.Aligned = true
	}
	return res
}

// onAnomaly 处理异常样本：不计入估计；连续异常达到阈值触发时钟重建。
func (d *DeviceClock) onAnomaly(rawTS, arrival time.Time, res Result) Result {
	d.consecBad++
	log.Printf("[clockalign] anomaly=%s raw=%s arrival=%s consecBad=%d",
		res.Anomaly, rawTS.Format(time.RFC3339Nano), arrival.Format(time.RFC3339Nano), d.consecBad)

	if d.consecBad >= d.cfg.ResetThreshold {
		// 时钟跳变/重启重新计时：重建时间序，不沿用故障前的偏差与依据。
		log.Printf("[clockalign] clock jump/reset detected after %d consecutive anomalies, rebuilding", d.consecBad)
		d.Reset()
		// 设备从零重新计时：新纪元早于默认硬下界，下调本设备下界以接受新纪元。
		if rawTS.Before(d.invalidBefore) {
			d.invalidBefore = rawTS.Add(-time.Minute)
			log.Printf("[clockalign] device epoch restarted near zero, lowering invalid bound to %s",
				d.invalidBefore.Format(time.RFC3339Nano))
		}
		res.Reset = true
		res.Anomaly = AnomalyJump
		// 就地以当前样本重新学习。
		d.offset = arrival.Sub(rawTS)
		d.samples = 1
		d.consecBad = 0
		return res
	}

	// 未触发重建：报告长时间无法对齐（供采集层降级）。
	if !d.aligned && !d.unaligned && arrival.Sub(d.sinceReset) > time.Duration(d.cfg.AlignTimeoutMs)*time.Millisecond {
		d.unaligned = true
		log.Printf("[clockalign] unable to align for %v, reporting unaligned", arrival.Sub(d.sinceReset))
	}
	res.Unaligned = d.unaligned
	res.Aligned = d.aligned
	res.Offset = d.offset
	return res
}
