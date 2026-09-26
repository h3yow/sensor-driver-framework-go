// demo 演示多传感器并发采集：理想设备、半包+粘包设备、坏帧设备、
// 乱序设备与阻塞设备同时上线，观察判定结果与健康度迁移。
package main

import (
	"log"
	"time"

	"github.com/h3yow/sensor-driver-framework-go/pkg/acquire"
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
	}
	m := acquire.NewManager()
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
	go func() {
		for r := range out {
			c := counts[r.Device]
			if c == nil {
				c = make(map[acquire.Quality]int)
				counts[r.Device] = c
			}
			c[r.Quality]++
		}
	}()

	time.Sleep(5 * time.Second)
	close(stop)
	time.Sleep(300 * time.Millisecond)

	log.Println("==== summary ====")
	for name := range devs {
		log.Printf("dev=%s health=%s qualities=%v", name, m.HealthOf(name), counts[name])
	}
}
