// 演示：纯本地运行的多传感器采集驱动。
//
// 运行：go run ./cmd/demo
//
// 三台可编程模拟设备分别覆盖：
//   - temp-a：半包/粘包后恢复正常，全程健康；
//   - temp-b：间歇坏帧导致健康度降级；
//   - temp-c：首次连接阻塞被隔离，第二次连接恢复。
package main

import (
	"context"
	"os"
	"time"

	"github.com/h3yow/sensor-driver-framework-go/driver"
	"github.com/h3yow/sensor-driver-framework-go/frame"
	"github.com/h3yow/sensor-driver-framework-go/logging"
	"github.com/h3yow/sensor-driver-framework-go/sensor"
	"github.com/h3yow/sensor-driver-framework-go/sim"
)

func reading(seq uint16, v float64) sim.Step {
	return sim.Step{Kind: sim.StepSendFrame, Frame: frame.Frame{
		Type:    frame.TypeReading,
		Seq:     seq,
		TS:      time.Now().UnixMilli(),
		Payload: sensor.EncodeReadingPayload(v),
	}}
}

func main() {
	logger := logging.New(logging.Options{Writer: os.Stdout, Prefix: "demo", Debug: false})

	// 设备 A：半包 + 粘包开场，之后周期发正常读数。
	devA := sim.New(func(attempt int) sim.Script {
		base := time.Now().UnixMilli()
		f1 := frame.Frame{Type: frame.TypeReading, Seq: 1, TS: base,
			Payload: sensor.EncodeReadingPayload(21.5)}
		f2 := frame.Frame{Type: frame.TypeReading, Seq: 2, TS: base + 10,
			Payload: sensor.EncodeReadingPayload(21.6)}
		steps := []sim.Step{
			{Kind: sim.StepSendFrame, Frame: f1, Split: true, SplitDelay: 30 * time.Millisecond},
			{Kind: sim.StepSendFrame, Frame: f2, PackWithNext: true},
			{Kind: sim.StepSendFrame, Frame: frame.Frame{Type: frame.TypeReading, Seq: 3,
				TS: base + 20, Payload: sensor.EncodeReadingPayload(21.7)}},
		}
		for i := 0; i < 60; i++ {
			steps = append(steps,
				sim.Step{Kind: sim.StepSleep, Sleep: 100 * time.Millisecond},
				sim.Step{Kind: sim.StepSendFrame, Frame: frame.Frame{
					Type: frame.TypeReading, Seq: uint16(4 + i),
					TS:      base + int64(30+100*(i+1)),
					Payload: sensor.EncodeReadingPayload(21.0 + float64(i%5)*0.1),
				}})
		}
		return sim.Script{Steps: append(steps, sim.Step{Kind: sim.StepBlock})}
	})

	// 设备 B：先发好帧变 Healthy，再注入若干坏帧触发降级。
	devB := sim.New(func(attempt int) sim.Script {
		good, _ := frame.Encode(&frame.Frame{Type: frame.TypeReading, Seq: 1,
			TS: time.Now().UnixMilli(), Payload: sensor.EncodeReadingPayload(18.0)})
		var steps []sim.Step
		bbase := time.Now().UnixMilli()
		for i := 0; i < 25; i++ {
			steps = append(steps, sim.Step{Kind: sim.StepSendFrame, Frame: frame.Frame{
				Type: frame.TypeReading, Seq: uint16(i + 1), TS: bbase + int64(i),
				Payload: sensor.EncodeReadingPayload(18.0 + float64(i%3)*0.05),
			}})
		}
		for i := 0; i < 20; i++ {
			bad := append([]byte(nil), good...)
			bad[15] ^= byte(i + 1) // 破坏 payload，CRC 必然失败
			steps = append(steps, sim.Step{Kind: sim.StepSendRaw, Raw: bad})
		}
		return sim.Script{Steps: append(steps, sim.Step{Kind: sim.StepSleep, Sleep: 30 * time.Second})}
	})

	// 设备 C：首次连接直接静默阻塞；重连后正常。
	devC := sim.New(func(attempt int) sim.Script {
		if attempt == 1 {
			return sim.Script{Steps: []sim.Step{{Kind: sim.StepBlock}}}
		}
		var steps []sim.Step
		cbase := time.Now().UnixMilli()
		for i := 0; i < 60; i++ {
			steps = append(steps,
				sim.Step{Kind: sim.StepSendFrame, Frame: frame.Frame{
					Type: frame.TypeReading, Seq: uint16(i + 1), TS: cbase + int64(100*(i+1)),
					Payload: sensor.EncodeReadingPayload(1010.0 + float64(i%4)*0.2),
				}},
				sim.Step{Kind: sim.StepSleep, Sleep: 100 * time.Millisecond})
		}
		return sim.Script{Steps: append(steps, sim.Step{Kind: sim.StepBlock})}
	})

	specT := sensor.Spec{Kind: sensor.KindTemperature, Min: -40, Max: 80, MaxAge: 5 * time.Second}
	specP := sensor.Spec{Kind: sensor.KindPressure, Min: 800, Max: 1200, MaxAge: 5 * time.Second}

	m := driver.NewManager(driver.WithLogger(logger))
	must(m.Register(driver.DeviceConfig{
		ID: "temp-a", Kind: sensor.KindTemperature, Dialer: devA.Dialer(), Spec: specT,
		Reconnect: driver.BackoffConfig{Initial: 50 * time.Millisecond, Max: time.Second, Factor: 2},
	}))
	must(m.Register(driver.DeviceConfig{
		ID: "temp-b", Kind: sensor.KindTemperature, Dialer: devB.Dialer(), Spec: specT,
		BadWindow: 20, MaxBadRate: 0.5,
		// 坏帧后设备静默：放宽空闲检测，使本设备只演示“坏帧率降级”而非阻塞隔离。
		IdleTimeout: 30 * time.Second, MaxStalls: 100,
		Reconnect: driver.BackoffConfig{Initial: 50 * time.Millisecond, Max: time.Second, Factor: 2},
	}))
	must(m.Register(driver.DeviceConfig{
		ID: "press-c", Kind: sensor.KindPressure, Dialer: devC.Dialer(), Spec: specP,
		IdleTimeout: 200 * time.Millisecond, MaxStalls: 2,
		Reconnect: driver.BackoffConfig{Initial: 50 * time.Millisecond, Max: time.Second, Factor: 2},
	}))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	must(m.Start(ctx))

	// 观察 3 秒内的状态快照与读数。
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		time.Sleep(500 * time.Millisecond)
		logger.Info("status snapshot")
		for _, st := range m.Status() {
			logger.Info("device",
				"id", st.ID, "health", st.Health.String(), "session", st.SessionState.String(),
				"good", st.Counters.GoodFrames, "bad", st.Counters.BadFrames,
				"accepted", st.Counters.Accepted, "stalls", st.Counters.IdleTimeouts,
				"connects", st.Counters.Connects)
		}
	}
	m.Stop()
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
