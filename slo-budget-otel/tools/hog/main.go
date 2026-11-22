// Command hog burns CPU for a fixed time: `threads` OS threads, each busy for `duty` of
// every 1 ms slice, and optionally one more thread busy for `extra-duty` of every
// `extra-period` (late_everywhere: 6 full threads plus a seventh spinning a fraction f of
// every 10 ms, a load between 6 and 7 CPUs that sets no CPU limit on the container).  It
// is copied into a target container (docker cp) and started there (docker exec -d), so
// the spinning threads share the service's cgroup; it exits by itself after -dur.
// Static build: CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o hog .
package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"sync"
	"time"
)

// spin keeps one locked OS thread busy for `busy` of every `busy + idle` until stop.
func spin(busy, idle time.Duration, stop time.Time, wg *sync.WaitGroup) {
	defer wg.Done()
	runtime.LockOSThread()
	x := uint64(1)
	for time.Now().Before(stop) {
		end := time.Now().Add(busy)
		for time.Now().Before(end) {
			for k := 0; k < 2000; k++ {
				x = x*6364136223846793005 + 1442695040888963407
			}
		}
		if idle > 0 {
			time.Sleep(idle)
		}
	}
	if x == 0 {
		fmt.Println(x)
	}
}

func main() {
	threads := flag.Int("threads", 8, "spinning OS threads")
	duty := flag.Float64("duty", 1.0, "fraction of every 1 ms slice a thread spins (0..1]")
	extraDuty := flag.Float64("extra-duty", 0, "one more thread spinning this fraction of every extra-period (0: none)")
	extraPeriod := flag.Duration("extra-period", 10*time.Millisecond, "the extra thread's cycle")
	dur := flag.Duration("dur", 180*time.Second, "how long to run")
	flag.Parse()
	if *threads < 1 || *duty <= 0 || *duty > 1 || *extraDuty < 0 || *extraDuty > 1 || *extraPeriod <= 0 {
		fmt.Fprintln(os.Stderr, "hog: threads >= 1, 0 < duty <= 1, 0 <= extra-duty <= 1, extra-period > 0")
		os.Exit(2)
	}
	runtime.GOMAXPROCS(*threads + 2)
	stop := time.Now().Add(*dur)
	busy := time.Duration(float64(time.Millisecond) * *duty)
	var wg sync.WaitGroup
	for i := 0; i < *threads; i++ {
		wg.Add(1)
		go spin(busy, time.Millisecond-busy, stop, &wg)
	}
	if *extraDuty > 0 {
		eb := time.Duration(float64(*extraPeriod) * *extraDuty)
		wg.Add(1)
		go spin(eb, *extraPeriod-eb, stop, &wg)
	}
	wg.Wait()
}
