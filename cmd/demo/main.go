// demo 演示多传感器并发采集：理想设备、半包+粘包设备、坏帧设备、
// 乱序设备、阻塞设备，以及时钟漂移/跳变/重启设备同时上线，
// 观察时间对齐、时钟异常判定结果与健康度迁移。
package main

import (
	"log"
	"time"

	"github.com/h3yow/sensor-driver-framework-go/pkg/acquire"
	"github.com/h3yow/sensor-driver-framework-go/pkg/simdev"
	"github.com/h3yow/sensor-driver-framework-go/pkg/timesync"
)

func main() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	devs := map[string]simdev.Behavior{
		"ideal":   {},
		"framing": {SplitFrames: true, Sticky: true},
		"corrupt": {CorruptEvery: 3},
		"reorder": {ReorderWindow: 3},
		"blocker": {BlockAfter: 10, BlockMs: 800},
		// 时钟异常演示：漂移、尖峰、跳变、重启重计
		"drift": {TsOffsetMs: 800, TsDriftMsPerSec: 50},
		"spike": {TsSpikeEvery: 20, TsSpikeMs: 120000},
		"jump":  {TsJumpAfter: 100, TsJumpMs: 300000},
	}
	m := acquire.NewManagerWithConfig(acquire.Config{
		TimeSync: &timesync.Config{}, // 全部默认：3 样本对齐，1s 接受窗口，30s 超前/滞后阈值
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
		log.Printf("dev=%s health=%s qualities=%v adjusted=%d", name, m.HealthOf(name), counts[name], adjusted[name])
	}
}
