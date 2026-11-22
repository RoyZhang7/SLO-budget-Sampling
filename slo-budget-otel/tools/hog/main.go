// Command hog burns CPU for a fixed time: `threads` OS threads, each busy for `duty` of
// every 1 ms slice.  It is copied into a target container (docker cp) and started there
// (docker exec -d), so the spinning threads share the service's cgroup; it exits by itself
// after -dur.  Static build: CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o hog .
package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"sync"
	"time"
)

func main() {
	threads := flag.Int("threads", 8, "spinning OS threads")
	duty := flag.Float64("duty", 1.0, "fraction of every 1 ms slice a thread spins (0..1]")
	dur := flag.Duration("dur", 180*time.Second, "how long to run")
	flag.Parse()
	if *threads < 1 || *duty <= 0 || *duty > 1 {
		fmt.Fprintln(os.Stderr, "hog: threads >= 1 and 0 < duty <= 1")
		os.Exit(2)
	}
	runtime.GOMAXPROCS(*threads + 1)
	stop := time.Now().Add(*dur)
	busy := time.Duration(float64(time.Millisecond) * *duty)
	idle := time.Millisecond - busy
	var wg sync.WaitGroup
	for i := 0; i < *threads; i++ {
		wg.Add(1)
		go func() {
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
		}()
	}
	wg.Wait()
}
