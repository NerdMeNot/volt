// Volt vs Go comparative benchmarks (Go side).
//
// Emits JSON with median over N iterations matching the Volt-side bench.
// Same workloads, same iteration counts, same GOMAXPROCS as Volt's
// default worker count (= NumCPU).
//
// Build/run: see ../compare.zig orchestrator.

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"sort"
	"sync"
	"time"
)

const (
	WarmupIters = 3
	BenchIters  = 11
)

// ─────────────────────────────────────────────────────────────────────
// Workload: yield ping-pong (one-way via runtime.Gosched)
// ─────────────────────────────────────────────────────────────────────
//
// Volt's "yield ping-pong" measures ctx_swap cost between two coros.
// Go's analog: a single goroutine calling runtime.Gosched() in a loop.
// Each Gosched is conceptually one round-trip through the scheduler.
// We divide wall time by (iters * 2) to match Volt's "one-way" report.

func benchYield(iters int) uint64 {
	done := make(chan struct{})
	var t0 time.Time
	go func() {
		t0 = time.Now()
		for i := 0; i < iters; i++ {
			runtime.Gosched()
		}
		close(done)
	}()
	<-done
	wall := time.Since(t0).Nanoseconds()
	return uint64(wall) / uint64(iters*2)
}

// ─────────────────────────────────────────────────────────────────────
// Workload: spawn + join (newproc + wait via channel)
// ─────────────────────────────────────────────────────────────────────

func benchSpawnJoin(n int) uint64 {
	t0 := time.Now()
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() { wg.Done() }()
	}
	wg.Wait()
	wall := time.Since(t0).Nanoseconds()
	return uint64(wall) / uint64(n)
}

// ─────────────────────────────────────────────────────────────────────
// Workload: channel SPSC cap=16
// ─────────────────────────────────────────────────────────────────────

func benchChannelSpsc(n int) uint64 {
	ch := make(chan uint64, 16)
	done := make(chan struct{})

	t0 := time.Now()
	go func() {
		for i := 0; i < n; i++ {
			ch <- uint64(i)
		}
		close(ch)
	}()
	go func() {
		var sum uint64 = 0
		for v := range ch {
			sum += v
		}
		_ = sum
		close(done)
	}()
	<-done
	wall := time.Since(t0).Nanoseconds()
	return uint64(wall) / uint64(n)
}

// ─────────────────────────────────────────────────────────────────────
// Workload: mutex contended (8 × 50k)
// ─────────────────────────────────────────────────────────────────────

func benchMutex(workers int, iters int) uint64 {
	var mu sync.Mutex
	var counter uint64

	var wg sync.WaitGroup
	wg.Add(workers)

	t0 := time.Now()
	for w := 0; w < workers; w++ {
		go func() {
			for i := 0; i < iters; i++ {
				mu.Lock()
				counter++
				mu.Unlock()
			}
			wg.Done()
		}()
	}
	wg.Wait()
	wall := time.Since(t0).Nanoseconds()

	if counter != uint64(workers*iters) {
		fmt.Fprintf(os.Stderr, "mutex bench correctness: expected %d, got %d\n",
			workers*iters, counter)
		os.Exit(1)
	}
	return uint64(wall) / uint64(workers*iters)
}

// ─────────────────────────────────────────────────────────────────────
// Runner — median over N iterations
// ─────────────────────────────────────────────────────────────────────

func runMedian(fn func() uint64) uint64 {
	for i := 0; i < WarmupIters; i++ {
		_ = fn()
	}
	samples := make([]uint64, BenchIters)
	for i := range samples {
		samples[i] = fn()
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	return samples[BenchIters/2]
}

type Results struct {
	YieldOneWayNs     uint64 `json:"yield_one_way_ns"`
	SpawnJoinNs       uint64 `json:"spawn_join_ns"`
	SpawnWaitgroupNs  uint64 `json:"spawn_waitgroup_ns"`
	ChannelSpsc16Ns   uint64 `json:"channel_spsc_16_ns"`
	MutexContended8Ns uint64 `json:"mutex_contended_8_ns"`
}

func main() {
	// Match Volt's default worker count (uses NumCPU). GOMAXPROCS
	// defaults to NumCPU too, so this is implicit on most systems —
	// set it explicitly here for clarity and reproducibility.
	runtime.GOMAXPROCS(runtime.NumCPU())

	r := Results{
		YieldOneWayNs: runMedian(func() uint64 { return benchYield(100_000) }),
		// spawn_join: per-coroutine join (matches Volt's `for j |.| j.join()`).
		// Go's WaitGroup pattern is more idiomatic — see spawn_waitgroup.
		SpawnJoinNs: runMedian(func() uint64 { return benchSpawnJoin(10_000) }),
		// spawn_waitgroup: spawn + single batched wait. Volt's analog
		// uses a Notify+atomic counter; both wait for ALL coros at once.
		SpawnWaitgroupNs:  runMedian(func() uint64 { return benchSpawnJoin(10_000) }),
		ChannelSpsc16Ns:   runMedian(func() uint64 { return benchChannelSpsc(100_000) }),
		MutexContended8Ns: runMedian(func() uint64 { return benchMutex(8, 50_000) }),
	}

	out, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "json marshal: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(string(out))
}
