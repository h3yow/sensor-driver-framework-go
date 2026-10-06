// Package timesync 实现单设备时钟相对统一时间基准的对齐与异常防护。
//
// 每台设备一个 Aligner：以 (到达时间 - 设备时间) 为偏差样本，用定长环形
// 缓冲 + 中位数估计偏差，能跟随缓慢漂移；偏离估计的样本被拒绝，个别离群
// 读数不会带偏估计；持续偏离判定为时钟跳变并重建时间序。状态按设备独立
// 维护，内存占用有界（环形缓冲定长）。
package timesync

import (
	"log"
	"sort"
	"time"
)

// State 表示对齐状态。
type State int

const (
	StateAligning State = iota // 样本不足，尚未建立对齐
	StateAligned               // 已对齐，偏差估计可用
	StateDegraded              // 长时间无法对齐，数据应被限制进入结果
)

func (s State) String() string {
	switch s {
	case StateAligning:
		return "Aligning"
	case StateAligned:
		return "Aligned"
	case StateDegraded:
		return "Degraded"
	}
	return "Unknown"
}

// Reason 是时钟异常的可区分判定原因。
type Reason int

const (
	ReasonNone    Reason = iota // 无异常
	ReasonAhead                 // 明显超前
	ReasonBehind                // 明显滞后
	ReasonInvalid               // 完全不合理（零值/负值等）
	ReasonOutlier               // 偏离估计但未达超前/滞后阈值
)

func (r Reason) String() string {
	switch r {
	case ReasonNone:
		return "None"
	case ReasonAhead:
		return "Ahead"
	case ReasonBehind:
		return "Behind"
	case ReasonInvalid:
		return "Invalid"
	case ReasonOutlier:
		return "Outlier"
	}
	return "Unknown"
}

// Config 是对齐与异常判定参数（零值取默认值）。
type Config struct {
	MinSamples     int       // 建立对齐所需的一致样本数（默认 3）
	AcceptWindowMs int       // 残差接受窗口，超出则不更新滤波（默认 1000ms）
	AheadMs        int       // 明显超前阈值（默认 30000ms）
	BehindMs       int       // 明显滞后阈值（默认 30000ms）
	JumpConfirm    int       // 连续被拒绝样本达到该数判定为跳变（默认 3）
	AlignTimeoutMs int       // 超过该时长仍无法对齐则降级（默认 10000ms）
	SampleBufSize  int       // 样本环形缓冲大小（默认 32，内存有界）
	MinValidTime   time.Time // 绝对有效下界（默认 1970-01-01，即仅拒绝零值/负值）
}

func (c *Config) withDefaults() Config {
	out := *c
	if out.MinSamples <= 0 {
		out.MinSamples = 3
	}
	if out.AcceptWindowMs <= 0 {
		out.AcceptWindowMs = 1000
	}
	if out.AheadMs <= 0 {
		out.AheadMs = 30000
	}
	if out.BehindMs <= 0 {
		out.BehindMs = 30000
	}
	if out.JumpConfirm <= 0 {
		out.JumpConfirm = 3
	}
	if out.AlignTimeoutMs <= 0 {
		out.AlignTimeoutMs = 10000
	}
	if out.SampleBufSize <= 0 {
		out.SampleBufSize = 32
	}
	if out.MinValidTime.IsZero() {
		out.MinValidTime = time.Unix(0, 0) // 默认仅拒绝零值/负值时间戳
	}
	return out
}

// Result 是一次观测的对齐结果。
type Result struct {
	AlignedTS time.Time     // 统一时间轴上的时间（Adjusted 为 true 时有效）
	Adjusted  bool          // 时间是否被对齐调整过
	State     State         // 观测后的对齐状态
	Reason    Reason        // 异常判定原因（None 表示无异常）
	Jump      bool          // 本次观测是否触发了时间序重建
	Offset    time.Duration // 当前估计偏差（统一时间 ≈ 设备时间 + Offset）
}

// Aligner 是单设备的时间对齐器（非并发安全，由设备采集 goroutine 独占）。
type Aligner struct {
	cfg Config

	state  State
	ring   []int64 // 已接受偏差样本（纳秒）环形缓冲
	head   int     // 下一个写入位置
	count  int     // 当前样本数（<= len(ring)）
	offset int64   // 当前偏差估计（样本中位数）

	consecRejected int       // 连续被拒绝样本数（用于跳变判定）
	aligningSince  time.Time // 进入 Aligning 的时刻
}

// New 创建对齐器。
func New(cfg Config) *Aligner {
	c := cfg.withDefaults()
	return &Aligner{cfg: c, state: StateAligning, ring: make([]int64, c.SampleBufSize)}
}

// Reset 重建时间序（用于重连/重启后）：丢弃全部偏差估计与判定依据。
func (a *Aligner) Reset() {
	log.Printf("[timesync] reset: state=%s offset=%v discarded", a.state, time.Duration(a.offset))
	a.state = StateAligning
	a.head, a.count, a.offset = 0, 0, 0
	a.consecRejected = 0
	a.aligningSince = time.Time{}
}

// State 返回当前对齐状态。
func (a *Aligner) State() State { return a.state }

// Offset 返回当前估计偏差（统一时间 ≈ 设备时间 + Offset）。
func (a *Aligner) Offset() time.Duration { return time.Duration(a.offset) }

// Observe 观测一条读数：deviceTS 为设备时间戳，arrivalTS 为到达（基准）时间。
func (a *Aligner) Observe(deviceTS, arrivalTS time.Time) Result {
	// 完全不合理的时间戳：不进入任何统计，也不影响后续判定。
	if deviceTS.IsZero() || deviceTS.Before(a.cfg.MinValidTime) {
		log.Printf("[timesync] invalid device ts=%s (min valid %s)",
			deviceTS.Format(time.RFC3339Nano), a.cfg.MinValidTime.Format(time.RFC3339Nano))
		return Result{State: a.state, Reason: ReasonInvalid, Offset: a.Offset()}
	}
	sample := arrivalTS.Sub(deviceTS).Nanoseconds()

	if a.state != StateAligned {
		return a.observeUnaligned(sample, deviceTS, arrivalTS)
	}
	return a.observeAligned(sample, deviceTS, arrivalTS)
}

// observeUnaligned 处理未对齐（Aligning/Degraded）状态下的样本。
func (a *Aligner) observeUnaligned(sample int64, deviceTS, now time.Time) Result {
	if a.aligningSince.IsZero() {
		a.aligningSince = now
	}
	a.push(sample)
	if a.count >= a.cfg.MinSamples {
		lo, hi := a.span()
		if hi-lo <= int64(a.cfg.AcceptWindowMs)*int64(time.Millisecond) {
			a.offset = a.median()
			old := a.state
			a.state = StateAligned
			a.consecRejected = 0
			log.Printf("[timesync] aligned (%s -> Aligned): offset=%v samples=%d",
				old, a.Offset(), a.count)
			// 完成对齐的这条读数本身也可给出统一时间
			return Result{AlignedTS: deviceTS.Add(a.Offset()), Adjusted: true, State: a.state, Offset: a.Offset()}
		}
	}
	if a.state == StateAligning &&
		now.Sub(a.aligningSince) > time.Duration(a.cfg.AlignTimeoutMs)*time.Millisecond {
		a.state = StateDegraded
		// 既有样本互相矛盾、不可信：清空重新积累，使恢复后能快速重新对齐
		a.head, a.count = 0, 0
		log.Printf("[timesync] cannot align within %dms, state -> Degraded", a.cfg.AlignTimeoutMs)
	}
	return Result{State: a.state, Offset: a.Offset()}
}

// observeAligned 处理已对齐状态下的样本：分类、滤波、跳变检测。
func (a *Aligner) observeAligned(sample int64, deviceTS, arrivalTS time.Time) Result {
	residual := sample - a.offset
	accept := int64(a.cfg.AcceptWindowMs) * int64(time.Millisecond)
	if residual >= -accept && residual <= accept {
		a.push(sample)
		a.offset = a.median()
		a.consecRejected = 0
		aligned := deviceTS.Add(a.Offset())
		return Result{AlignedTS: aligned, Adjusted: true, State: a.state, Offset: a.Offset()}
	}
	// 拒绝：分类原因，不更新滤波，异常样本不成为比较基准。
	// 残差 = 偏差样本 - 估计；残差为负表示设备时间戳比预期超前。
	var reason Reason
	switch {
	case residual < -int64(a.cfg.AheadMs)*int64(time.Millisecond):
		reason = ReasonAhead // 设备时间戳明显超前
	case residual > int64(a.cfg.BehindMs)*int64(time.Millisecond):
		reason = ReasonBehind // 设备时间戳明显滞后
	default:
		reason = ReasonOutlier
	}
	a.consecRejected++
	jump := false
	if a.consecRejected >= a.cfg.JumpConfirm {
		// 持续偏离：判定为时钟跳变，重建时间序。
		log.Printf("[timesync] clock jump detected after %d rejected samples (residual=%v), re-establishing",
			a.consecRejected, time.Duration(residual))
		a.Reset()
		a.aligningSince = arrivalTS
		a.push(sample) // 用当前样本作为新时间序的种子
		jump = true
	} else {
		log.Printf("[timesync] rejected sample: reason=%s residual=%v consec=%d",
			reason, time.Duration(residual), a.consecRejected)
	}
	return Result{State: a.state, Reason: reason, Jump: jump, Offset: a.Offset()}
}

// push 将样本写入环形缓冲（覆盖最旧样本，内存有界）。
func (a *Aligner) push(sample int64) {
	a.ring[a.head] = sample
	a.head = (a.head + 1) % len(a.ring)
	if a.count < len(a.ring) {
		a.count++
	}
}

// span 返回当前样本的最小值与最大值。
func (a *Aligner) span() (lo, hi int64) {
	lo, hi = a.ring[0], a.ring[0]
	for i := 1; i < a.count; i++ {
		if a.ring[i] < lo {
			lo = a.ring[i]
		}
		if a.ring[i] > hi {
			hi = a.ring[i]
		}
	}
	return lo, hi
}

// median 返回当前样本的中位数。
func (a *Aligner) median() int64 {
	tmp := make([]int64, a.count)
	copy(tmp, a.ring[:a.count])
	sort.Slice(tmp, func(i, j int) bool { return tmp[i] < tmp[j] })
	return tmp[len(tmp)/2]
}
