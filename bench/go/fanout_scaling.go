// Go side-by-side for Volt's bench-fanout-scaling.
//
// Spawns N driver goroutines, each running its own spawn-WaitGroup-Wait
// loop. Matches the Zig shape: drivers == GOMAXPROCS, BATCH=100,
// duration=4s/sweep.
//
// Usage:
//   GOMAXPROCS=N go run fanout_scaling.go   # one-shot at N
//   go run fanout_scaling.go                # sweep over 1,2,4,8,NumCPU

package main

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const (
	durationS = 4
	batch     = 100
)

func nopFn() {}

type driverCtx struct {
	batch      uint32
	deadlineNs int64
	ops        atomic.Uint64
}

func driver(ctx *driverCtx) {
	wg := sync.WaitGroup{}
	var localOps uint64
	for time.Now().UnixNano() < ctx.deadlineNs {
		wg.Add(int(ctx.batch))
		for i := uint32(0); i < ctx.batch; i++ {
			go func() {
				nopFn()
				wg.Done()
			}()
		}
		wg.Wait()
		localOps += uint64(ctx.batch)
	}
	ctx.ops.Add(localOps)
}

type sample struct {
	drivers   int
	workers   int
	totalOps  uint64
	elapsedNs int64
}

func runOnce(drivers, workers int) sample {
	runtime.GOMAXPROCS(workers)
	ctx := &driverCtx{batch: batch, deadlineNs: time.Now().UnixNano() + int64(durationS)*int64(time.Second)}
	wg := sync.WaitGroup{}
	wg.Add(drivers)
	start := time.Now()
	for i := 0; i < drivers; i++ {
		go func() {
			driver(ctx)
			wg.Done()
		}()
	}
	wg.Wait()
	return sample{
		drivers:   drivers,
		workers:   workers,
		totalOps:  ctx.ops.Load(),
		elapsedNs: time.Since(start).Nanoseconds(),
	}
}

func main() {
	ncpu := runtime.NumCPU()
	fmt.Println("=== Go fan-out scaling (multiple drivers) ===")
	fmt.Printf("Platform: go1.26.0 darwin/arm64, batch=%d, duration=%ds, ncpu=%d\n", batch, durationS, ncpu)
	fmt.Printf("Each driver: spawn %d goroutines, WaitGroup.Wait, repeat for %ds.\n\n", batch, durationS)

	// One-shot mode if user passed an explicit worker count.
	if len(os.Args) >= 2 {
		w, _ := strconv.Atoi(os.Args[1])
		drivers := w
		if len(os.Args) >= 3 {
			drivers, _ = strconv.Atoi(os.Args[2])
		}
		s := runOnce(drivers, w)
		nsPerOp := float64(s.elapsedNs) / float64(s.totalOps)
		fmt.Printf("  workers=%d drivers=%d  total_ops=%d  ns/op=%.1f\n", s.workers, s.drivers, s.totalOps, nsPerOp)
		return
	}

	fmt.Println("Sweep with drivers = workers (peak parallel work):")
	fmt.Printf("  %8s %10s %14s %12s %10s\n", "workers", "drivers", "total_ops", "ns/op", "ratio")
	widths := []int{1, 2, 4, 8, ncpu}
	var baseNsPerOp float64
	for _, w := range widths {
		s := runOnce(w, w)
		nsPerOp := float64(s.elapsedNs) / float64(s.totalOps)
		if baseNsPerOp == 0 {
			baseNsPerOp = nsPerOp
		}
		ratio := nsPerOp / baseNsPerOp
		fmt.Printf("  %8d %10d %14d %10.1f %9.2fx\n", s.workers, s.drivers, s.totalOps, nsPerOp, ratio)
	}

	fmt.Println()
	fmt.Println("Throughput sweep (drivers > workers — oversubscribed):")
	fmt.Printf("  %8s %10s %14s %12s\n", "workers", "drivers", "total_ops", "ops/sec")
	for _, w := range []int{1, 4, ncpu} {
		s := runOnce(w*4, w)
		opsPerSec := s.totalOps * uint64(time.Second.Nanoseconds()) / uint64(s.elapsedNs)
		fmt.Printf("  %8d %10d %14d %12d\n", s.workers, s.drivers, s.totalOps, opsPerSec)
	}
}
