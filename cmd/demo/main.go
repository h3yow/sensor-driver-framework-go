// demo 演示多传感器并发采集：理想设备、半包+粘包设备、坏帧设备、
// 乱序设备、阻塞设备，以及时钟偏快/运行中跳变/重启从零计时的设备同时上线，
// 观察时间对齐、时钟异常判定结果与健康度迁移。
package main

import (
	"log"
	"time"

	"github.com/h3yow/sensor-driver-framework-go/pkg/acquire"
	"github.com/h3yow/sensor-driver-framework-go/pkg/clockalign"
	"github.com/h3yow/sensor-driver-framework-go/pkg/simdev"
)

func main() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	devs := map[string]simdev.Behavior{
		"ideal":   {},
		"framing": {SplitFrames: true, Sticky: true},
		"corrupt": {CorruptEvery: 3},
		"reorder": {ReorderWindow: 3},
		"blocker": {BlockAfter: 10, BlockMs: 800},
		// 时钟异常设备
		"clkahead": {TsOffsetMs: 10000},                       // 时钟偏快 10s
		"clkjump":  {TsJumpAfter: 200, TsJumpMs: -3600000},    // 运行中回拨 1h
		"clkreset": {TsFromZero: true},                        // 重启后从零计时
	}
	m := acquire.NewManagerWithConfig(acquire.Config{
		Clock: clockalign.Config{Enabled: true},
	})
	for name, b := range devs {
		d, err := simdev.New(name, b, 0)
		if err != nil {
			log.Fatalf("start device %s: %v", name, err)
		}
		defer d.Close()
		m.Add(name, d.Addr)
	}

	stop := make(chan struct{})
	out := make(chan acquire.Reading, 4096)
	go m.Run(stop, out)

	// 汇总统计
	counts := make(map[string]map[acquire.Quality]int)
	adjusted := make(map[string]int)
	go func() {
		for r := range out {
			c := counts[r.Device]
			if c == nil {
				c = make(map[acquire.Quality]int)
				counts[r.Device] = c
			}
			c[r.Quality]++
			if r.Adjusted {
				adjusted[r.Device]++
			}
		}
	}()

	time.Sleep(5 * time.Second)
	close(stop)
	time.Sleep(300 * time.Millisecond)

	log.Println("==== summary ====")
	for name := range devs {
		off, ok := m.ClockOffsetOf(name)
		offStr := "n/a"
		if ok {
			offStr = off.String()
		}
		log.Printf("dev=%s health=%s clockOffset=%s adjusted=%d qualities=%v",
			name, m.HealthOf(name), offStr, adjusted[name], counts[name])
	}
}
