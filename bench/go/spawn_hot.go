// Go side-by-side for Volt's bench-spawn-hot.
//
// One driver goroutine spawns BATCH=1000 goroutines, WaitGroup.Wait, loops
// for `durationS`. Reports ns/op = wall_time / total_spawns.
//
// This is the false-parallelism shape: serial spawn/join under N
// GOMAXPROCS. Measures coordination overhead, not work-stealing.
//
// Usage:
//   GOMAXPROCS=N go run spawn_hot.go        # one-shot at N
//   go run spawn_hot.go                     # sweep over 1,2,4,8,NumCPU

package main

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"sync"
	"time"
)

const (
	durationS = 10
	batch     = 1000
)

func nopFn() {}

func runOnce(workers int) (uint64, time.Duration) {
	runtime.GOMAXPROCS(workers)
	deadline := time.Now().Add(time.Duration(durationS) * time.Second)
	// Warmup — three batches.
	for w := 0; w < 3; w++ {
		wg := sync.WaitGroup{}
		wg.Add(batch)
		for i := 0; i < batch; i++ {
			go func() {
				nopFn()
				wg.Done()
			}()
		}
		wg.Wait()
	}

	var totalOps uint64
	start := time.Now()
	for time.Now().Before(deadline) {
		wg := sync.WaitGroup{}
		wg.Add(batch)
		for i := 0; i < batch; i++ {
			go func() {
				nopFn()
				wg.Done()
			}()
		}
		wg.Wait()
		totalOps += batch
	}
	return totalOps, time.Since(start)
}

func main() {
	ncpu := runtime.NumCPU()
	fmt.Println("=== Go spawn+wait hot loop ===")
	fmt.Printf("Platform: go1.26.0 darwin/arm64, batch=%d, duration=%ds, ncpu=%d\n\n", batch, durationS, ncpu)

	if len(os.Args) >= 2 {
		w, _ := strconv.Atoi(os.Args[1])
		ops, elapsed := runOnce(w)
		nsPerOp := float64(elapsed.Nanoseconds()) / float64(ops)
		opsPerSec := uint64(ops) * uint64(time.Second.Nanoseconds()) / uint64(elapsed.Nanoseconds())
		fmt.Printf("  workers=%d  total_ops=%d  ns/op=%.1f  ops/sec=%d\n", w, ops, nsPerOp, opsPerSec)
		return
	}

	fmt.Printf("  %8s %14s %12s %12s\n", "workers", "total_ops", "ns/op", "ops/sec")
	for _, w := range []int{1, 2, 4, 8, ncpu} {
		ops, elapsed := runOnce(w)
		nsPerOp := float64(elapsed.Nanoseconds()) / float64(ops)
		opsPerSec := uint64(ops) * uint64(time.Second.Nanoseconds()) / uint64(elapsed.Nanoseconds())
		fmt.Printf("  %8d %14d %12.1f %12d\n", w, ops, nsPerOp, opsPerSec)
	}
}
