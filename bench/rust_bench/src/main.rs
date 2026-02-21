//! Tokio Benchmark Suite — Canonical reference for Volt comparison
//!
//! Methodology:
//!
//! TIER 1 — SYNC FAST PATH (1M ops):
//!   Single-thread, no runtime. Both sides use try_* synchronous APIs.
//!   Measures raw data structure cost: CAS, atomics, memory barriers.
//!
//! TIER 2 — CHANNEL FAST PATH (100K ops):
//!   Single-thread, no runtime. Both sides use try_send/try_recv.
//!   Measures channel buffer operations without scheduling overhead.
//!
//! TIER 3 — ASYNC MULTI-TASK (10K ops):
//!   Both sides use their async runtimes (Tokio / Volt).
//!   Measures real-world contention: scheduling, waking, backpressure.
//!   NO spin-wait loops — proper async .await throughout.
//!
//! Statistics: median of 10 iterations (5 warmup discarded).
//! ns/op = median_ns / ops_per_iter (single division, no double-counting).
//!
//! Run:     cargo run --release
//! JSON:    cargo run --release -- --json

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use tokio::sync::{broadcast, mpsc, oneshot, watch};
use tokio::sync::{Barrier, Mutex, Notify, OnceCell, RwLock, Semaphore};

// ============================================================================
// Allocation Tracking — global allocator wrapper
// ============================================================================

struct TrackingAllocator;

static ALLOC_BYTES: AtomicUsize = AtomicUsize::new(0);
static ALLOC_COUNT: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            ALLOC_BYTES.fetch_add(layout.size(), Ordering::Relaxed);
            ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
    }
}

#[global_allocator]
static GLOBAL: TrackingAllocator = TrackingAllocator;

fn reset_alloc_counters() {
    ALLOC_BYTES.store(0, Ordering::Relaxed);
    ALLOC_COUNT.store(0, Ordering::Relaxed);
}

fn get_alloc_bytes() -> usize {
    ALLOC_BYTES.load(Ordering::Relaxed)
}

fn get_alloc_count() -> usize {
    ALLOC_COUNT.load(Ordering::Relaxed)
}

// ============================================================================
// Configuration — MUST MATCH volt_bench.zig EXACTLY
// ============================================================================

// Tier 1: Sync fast path (very fast ops, need high count for measurable time)
const SYNC_OPS: usize = 1_000_000;

// Tier 2: Channel fast path (moderate cost, buffer operations)
const CHANNEL_OPS: usize = 100_000;

// Tier 3: Async multi-task (expensive, involves scheduling + contention)
const ASYNC_OPS: usize = 10_000;

// Shared config
const ITERATIONS: usize = 10;
const WARMUP: usize = 5;
const NUM_WORKERS: usize = 4;

// MPMC config
const MPMC_PRODUCERS: usize = 4;
const MPMC_CONSUMERS: usize = 4;
const MPMC_BUFFER: usize = 1_024; // Power-of-2 for optimal bitmask indexing

// Contended config
const CONTENDED_MUTEX_TASKS: usize = 4;
const CONTENDED_RWLOCK_READERS: usize = 4;
const CONTENDED_RWLOCK_WRITERS: usize = 2;
const CONTENDED_SEM_TASKS: usize = 8;
const CONTENDED_SEM_PERMITS: usize = 2;

// Batch config (for task_spawn_batch)
const BATCH_SIZE: usize = 100;

// ============================================================================
// Statistics — median-based (robust against outliers)
// ============================================================================

struct Stats {
    samples: Vec<u64>,
}

impl Stats {
    fn new() -> Self {
        Stats {
            samples: Vec::with_capacity(ITERATIONS),
        }
    }

    fn add(&mut self, ns: u64) {
        self.samples.push(ns);
    }

    fn median_ns(&self) -> u64 {
        if self.samples.is_empty() {
            return 0;
        }
        let mut sorted = self.samples.clone();
        sorted.sort_unstable();
        sorted[sorted.len() / 2]
    }

    fn min_ns(&self) -> u64 {
        self.samples.iter().copied().min().unwrap_or(0)
    }
}

struct BenchResult {
    stats: Stats,
    ops_per_iter: usize, // ops done in ONE iteration — ns/op = median_ns / this
    total_bytes: usize,  // bytes allocated in last timed iteration
    total_allocs: usize, // allocation calls in last timed iteration
}

impl BenchResult {
    fn ns_per_op(&self) -> f64 {
        let med = self.stats.median_ns() as f64;
        let ops = self.ops_per_iter as f64;
        if ops > 0.0 {
            med / ops
        } else {
            0.0
        }
    }

    fn ops_per_sec(&self) -> f64 {
        let ns = self.ns_per_op();
        if ns > 0.0 {
            1_000_000_000.0 / ns
        } else {
            0.0
        }
    }

    fn bytes_per_op(&self) -> f64 {
        if self.ops_per_iter == 0 {
            return 0.0;
        }
        self.total_bytes as f64 / self.ops_per_iter as f64
    }

    fn allocs_per_op(&self) -> f64 {
        if self.ops_per_iter == 0 {
            return 0.0;
        }
        self.total_allocs as f64 / self.ops_per_iter as f64
    }
}

// ============================================================================
// TIER 1: SYNC FAST PATH (1M ops, single-thread, try_* APIs)
// ============================================================================

fn bench_mutex() -> BenchResult {
    let mut stats = Stats::new();

    for _ in 0..WARMUP {
        let mutex = Mutex::new(());
        for _ in 0..SYNC_OPS {
            let _guard = mutex.try_lock().unwrap();
        }
    }

    for _ in 0..ITERATIONS {
        reset_alloc_counters();
        let mutex = Mutex::new(());
        let start = Instant::now();
        for _ in 0..SYNC_OPS {
            let _guard = mutex.try_lock().unwrap();
        }
        stats.add(start.elapsed().as_nanos() as u64);
    }

    BenchResult {
        stats,
        ops_per_iter: SYNC_OPS,
        total_bytes: get_alloc_bytes(),
        total_allocs: get_alloc_count(),
    }
}

fn bench_rwlock_read() -> BenchResult {
    let mut stats = Stats::new();

    for _ in 0..WARMUP {
        let lock = RwLock::new(());
        for _ in 0..SYNC_OPS {
            let _guard = lock.try_read().unwrap();
        }
    }

    for _ in 0..ITERATIONS {
        reset_alloc_counters();
        let lock = RwLock::new(());
        let start = Instant::now();
        for _ in 0..SYNC_OPS {
            let _guard = lock.try_read().unwrap();
        }
        stats.add(start.elapsed().as_nanos() as u64);
    }

    BenchResult {
        stats,
        ops_per_iter: SYNC_OPS,
        total_bytes: get_alloc_bytes(),
        total_allocs: get_alloc_count(),
    }
}

fn bench_rwlock_write() -> BenchResult {
    let mut stats = Stats::new();

    for _ in 0..WARMUP {
        let lock = RwLock::new(());
        for _ in 0..SYNC_OPS {
            let _guard = lock.try_write().unwrap();
        }
    }

    for _ in 0..ITERATIONS {
        reset_alloc_counters();
        let lock = RwLock::new(());
        let start = Instant::now();
        for _ in 0..SYNC_OPS {
            let _guard = lock.try_write().unwrap();
        }
        stats.add(start.elapsed().as_nanos() as u64);
    }

    BenchResult {
        stats,
        ops_per_iter: SYNC_OPS,
        total_bytes: get_alloc_bytes(),
        total_allocs: get_alloc_count(),
    }
}

fn bench_semaphore() -> BenchResult {
    let mut stats = Stats::new();

    for _ in 0..WARMUP {
        let sem = Semaphore::new(10);
        for _ in 0..SYNC_OPS {
            let _permit = sem.try_acquire().unwrap();
        }
    }

    for _ in 0..ITERATIONS {
        reset_alloc_counters();
        let sem = Semaphore::new(10);
        let start = Instant::now();
        for _ in 0..SYNC_OPS {
            let _permit = sem.try_acquire().unwrap();
        }
        stats.add(start.elapsed().as_nanos() as u64);
    }

    BenchResult {
        stats,
        ops_per_iter: SYNC_OPS,
        total_bytes: get_alloc_bytes(),
        total_allocs: get_alloc_count(),
    }
}

fn bench_oncecell_get() -> BenchResult {
    let mut stats = Stats::new();

    for _ in 0..WARMUP {
        let cell = OnceCell::new();
        cell.set(42u64).unwrap();
        for _ in 0..SYNC_OPS {
            std::hint::black_box(cell.get());
        }
    }

    for _ in 0..ITERATIONS {
        let cell = OnceCell::new();
        cell.set(42u64).unwrap();
        reset_alloc_counters();
        let start = Instant::now();
        for _ in 0..SYNC_OPS {
            std::hint::black_box(cell.get());
        }
        stats.add(start.elapsed().as_nanos() as u64);
    }

    BenchResult {
        stats,
        ops_per_iter: SYNC_OPS,
        total_bytes: get_alloc_bytes(),
        total_allocs: get_alloc_count(),
    }
}

fn bench_oncecell_set() -> BenchResult {
    let mut stats = Stats::new();

    for _ in 0..WARMUP {
        for i in 0..SYNC_OPS {
            let cell = OnceCell::new();
            let _ = cell.set(i as u64);
        }
    }

    for _ in 0..ITERATIONS {
        reset_alloc_counters();
        let start = Instant::now();
        for i in 0..SYNC_OPS {
            let cell = OnceCell::new();
            let _ = cell.set(i as u64);
        }
        stats.add(start.elapsed().as_nanos() as u64);
    }

    BenchResult {
        stats,
        ops_per_iter: SYNC_OPS,
        total_bytes: get_alloc_bytes(),
        total_allocs: get_alloc_count(),
    }
}

// Notify: Tokio has no sync consume API — .notified().await is the only way.
// The permit is pre-stored, so the .await completes on first poll.
async fn bench_notify() -> BenchResult {
    let mut stats = Stats::new();

    for _ in 0..WARMUP {
        for _ in 0..SYNC_OPS {
            let notify = Notify::new();
            notify.notify_one();
            notify.notified().await;
        }
    }

    for _ in 0..ITERATIONS {
        reset_alloc_counters();
        let start = Instant::now();
        for _ in 0..SYNC_OPS {
            let notify = Notify::new();
            notify.notify_one();
            notify.notified().await;
        }
        stats.add(start.elapsed().as_nanos() as u64);
    }

    BenchResult {
        stats,
        ops_per_iter: SYNC_OPS,
        total_bytes: get_alloc_bytes(),
        total_allocs: get_alloc_count(),
    }
}

// Barrier: Tokio has no sync wait — .wait().await is the only way.
// With 1 participant, the .await completes on first poll.
async fn bench_barrier() -> BenchResult {
    let mut stats = Stats::new();

    for _ in 0..WARMUP {
        for _ in 0..SYNC_OPS {
            let barrier = Barrier::new(1);
            barrier.wait().await;
        }
    }

    for _ in 0..ITERATIONS {
        reset_alloc_counters();
        let start = Instant::now();
        for _ in 0..SYNC_OPS {
            let barrier = Barrier::new(1);
            barrier.wait().await;
        }
        stats.add(start.elapsed().as_nanos() as u64);
    }

    BenchResult {
        stats,
        ops_per_iter: SYNC_OPS,
        total_bytes: get_alloc_bytes(),
        total_allocs: get_alloc_count(),
    }
}

// ============================================================================
// TIER 2: CHANNEL FAST PATH (100K ops, single-thread, try_send/try_recv)
// ============================================================================

fn bench_channel_send() -> BenchResult {
    let mut stats = Stats::new();

    for _ in 0..WARMUP {
        let (tx, mut rx) = mpsc::channel(CHANNEL_OPS);
        for i in 0..CHANNEL_OPS {
            tx.try_send(i as u64).unwrap();
        }
        for _ in 0..CHANNEL_OPS {
            let _ = rx.try_recv();
        }
    }

    for _ in 0..ITERATIONS {
        reset_alloc_counters();
        let (tx, mut rx) = mpsc::channel(CHANNEL_OPS);
        let start = Instant::now();
        for i in 0..CHANNEL_OPS {
            tx.try_send(i as u64).unwrap();
        }
        stats.add(start.elapsed().as_nanos() as u64);
        // Drain to avoid leak
        for _ in 0..CHANNEL_OPS {
            let _ = rx.try_recv();
        }
    }

    BenchResult {
        stats,
        ops_per_iter: CHANNEL_OPS,
        total_bytes: get_alloc_bytes(),
        total_allocs: get_alloc_count(),
    }
}

fn bench_channel_recv() -> BenchResult {
    let mut stats = Stats::new();

    for _ in 0..WARMUP {
        let (tx, mut rx) = mpsc::channel(CHANNEL_OPS);
        for i in 0..CHANNEL_OPS {
            tx.try_send(i as u64).unwrap();
        }
        for _ in 0..CHANNEL_OPS {
            let _ = rx.try_recv();
        }
    }

    for _ in 0..ITERATIONS {
        reset_alloc_counters();
        let (tx, mut rx) = mpsc::channel(CHANNEL_OPS);
        // Pre-fill channel
        for i in 0..CHANNEL_OPS {
            tx.try_send(i as u64).unwrap();
        }
        let start = Instant::now();
        for _ in 0..CHANNEL_OPS {
            let _ = rx.try_recv();
        }
        stats.add(start.elapsed().as_nanos() as u64);
    }

    BenchResult {
        stats,
        ops_per_iter: CHANNEL_OPS,
        total_bytes: get_alloc_bytes(),
        total_allocs: get_alloc_count(),
    }
}

fn bench_channel_roundtrip() -> BenchResult {
    let mut stats = Stats::new();

    for _ in 0..WARMUP {
        let (tx, mut rx) = mpsc::channel(1);
        for i in 0..CHANNEL_OPS {
            tx.try_send(i as u64).unwrap();
            let _ = rx.try_recv();
        }
    }

    for _ in 0..ITERATIONS {
        reset_alloc_counters();
        let (tx, mut rx) = mpsc::channel(1);
        let start = Instant::now();
        for i in 0..CHANNEL_OPS {
            tx.try_send(i as u64).unwrap();
            let _ = rx.try_recv();
        }
        stats.add(start.elapsed().as_nanos() as u64);
    }

    BenchResult {
        stats,
        ops_per_iter: CHANNEL_OPS,
        total_bytes: get_alloc_bytes(),
        total_allocs: get_alloc_count(),
    }
}

fn bench_oneshot() -> BenchResult {
    let mut stats = Stats::new();

    for _ in 0..WARMUP {
        for i in 0..CHANNEL_OPS {
            let (tx, mut rx) = oneshot::channel::<u64>();
            tx.send(i as u64).unwrap();
            std::hint::black_box(rx.try_recv().unwrap());
        }
    }

    for _ in 0..ITERATIONS {
        reset_alloc_counters();
        let start = Instant::now();
        for i in 0..CHANNEL_OPS {
            let (tx, mut rx) = oneshot::channel::<u64>();
            tx.send(i as u64).unwrap();
            std::hint::black_box(rx.try_recv().unwrap());
        }
        stats.add(start.elapsed().as_nanos() as u64);
    }

    BenchResult {
        stats,
        ops_per_iter: CHANNEL_OPS,
        total_bytes: get_alloc_bytes(),
        total_allocs: get_alloc_count(),
    }
}

fn bench_broadcast() -> BenchResult {
    let mut stats = Stats::new();
    let num_receivers: usize = 4;

    for _ in 0..WARMUP {
        let (tx, _) = broadcast::channel(CHANNEL_OPS);
        let mut receivers: Vec<_> = (0..num_receivers).map(|_| tx.subscribe()).collect();
        for i in 0..CHANNEL_OPS {
            let _ = tx.send(i as u64);
        }
        for rx in &mut receivers {
            for _ in 0..CHANNEL_OPS {
                let _ = rx.try_recv();
            }
        }
    }

    for _ in 0..ITERATIONS {
        reset_alloc_counters();
        let (tx, _) = broadcast::channel(CHANNEL_OPS);
        let mut receivers: Vec<_> = (0..num_receivers).map(|_| tx.subscribe()).collect();
        let start = Instant::now();
        for i in 0..CHANNEL_OPS {
            let _ = tx.send(i as u64);
        }
        for rx in &mut receivers {
            for _ in 0..CHANNEL_OPS {
                let _ = rx.try_recv();
            }
        }
        stats.add(start.elapsed().as_nanos() as u64);
    }

    BenchResult {
        stats,
        ops_per_iter: CHANNEL_OPS,
        total_bytes: get_alloc_bytes(),
        total_allocs: get_alloc_count(),
    }
}

fn bench_watch() -> BenchResult {
    let mut stats = Stats::new();

    for _ in 0..WARMUP {
        let (tx, rx) = watch::channel(0u64);
        for i in 0..CHANNEL_OPS {
            tx.send(i as u64).unwrap();
            std::hint::black_box(*rx.borrow());
        }
    }

    for _ in 0..ITERATIONS {
        reset_alloc_counters();
        let (tx, rx) = watch::channel(0u64);
        let start = Instant::now();
        for i in 0..CHANNEL_OPS {
            tx.send(i as u64).unwrap();
            std::hint::black_box(*rx.borrow());
        }
        stats.add(start.elapsed().as_nanos() as u64);
    }

    BenchResult {
        stats,
        ops_per_iter: CHANNEL_OPS,
        total_bytes: get_alloc_bytes(),
        total_allocs: get_alloc_count(),
    }
}

// ============================================================================
// TIER 3: ASYNC MULTI-TASK (10K ops, runtime required, NO spin-wait)
// ============================================================================

// MPMC: 4 producers + 4 consumers on async runtime.
// Channel capacity (1K) << total messages (10K) to force real backpressure.
// Uses .send().await / .recv().await — no busy-spinning.
async fn bench_channel_mpmc() -> BenchResult {
    let mut stats = Stats::new();
    let msgs_per_producer = ASYNC_OPS / MPMC_PRODUCERS;

    for _ in 0..WARMUP {
        run_mpmc_async(msgs_per_producer).await;
    }

    for _ in 0..ITERATIONS {
        reset_alloc_counters();
        let start = Instant::now();
        run_mpmc_async(msgs_per_producer).await;
        stats.add(start.elapsed().as_nanos() as u64);
    }

    BenchResult {
        stats,
        ops_per_iter: ASYNC_OPS,
        total_bytes: get_alloc_bytes(),
        total_allocs: get_alloc_count(),
    }
}

async fn run_mpmc_async(msgs_per_producer: usize) {
    let (tx, rx) = async_channel::bounded(MPMC_BUFFER);
    let mut handles = Vec::new();

    // Spawn producers — each sends msgs_per_producer values via .await
    for p in 0..MPMC_PRODUCERS {
        let tx = tx.clone();
        handles.push(tokio::spawn(async move {
            for i in 0..msgs_per_producer {
                tx.send((p * msgs_per_producer + i) as u64)
                    .await
                    .unwrap();
            }
        }));
    }
    drop(tx); // Channel closes when all producer clones are dropped

    // Spawn consumers — each receives via .await until channel closes
    for _ in 0..MPMC_CONSUMERS {
        let rx = rx.clone();
        handles.push(tokio::spawn(async move {
            while rx.recv().await.is_ok() {}
        }));
    }
    drop(rx);

    // Wait for all tasks to complete
    for h in handles {
        h.await.unwrap();
    }
}

// Contended Mutex: N tasks all lock().await the same mutex
async fn bench_mutex_contended() -> BenchResult {
    let mut stats = Stats::new();
    let ops_per_task = ASYNC_OPS / CONTENDED_MUTEX_TASKS;

    let mutex = Arc::new(Mutex::new(()));
    let counter = Arc::new(AtomicUsize::new(0));

    for _ in 0..WARMUP {
        counter.store(0, Ordering::SeqCst);
        let mut handles = Vec::new();
        for _ in 0..CONTENDED_MUTEX_TASKS {
            let mutex = mutex.clone();
            let counter = counter.clone();
            handles.push(tokio::spawn(async move {
                for _ in 0..ops_per_task {
                    let _guard = mutex.lock().await;
                    counter.fetch_add(1, Ordering::Relaxed);
                }
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
    }

    for _ in 0..ITERATIONS {
        reset_alloc_counters();
        counter.store(0, Ordering::SeqCst);
        let start = Instant::now();
        let mut handles = Vec::new();
        for _ in 0..CONTENDED_MUTEX_TASKS {
            let mutex = mutex.clone();
            let counter = counter.clone();
            handles.push(tokio::spawn(async move {
                for _ in 0..ops_per_task {
                    let _guard = mutex.lock().await;
                    counter.fetch_add(1, Ordering::Relaxed);
                }
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        stats.add(start.elapsed().as_nanos() as u64);
    }

    BenchResult {
        stats,
        ops_per_iter: ASYNC_OPS,
        total_bytes: get_alloc_bytes(),
        total_allocs: get_alloc_count(),
    }
}

// Contended RwLock: readers + writers contending via .await
async fn bench_rwlock_contended() -> BenchResult {
    let mut stats = Stats::new();
    let total_tasks = CONTENDED_RWLOCK_READERS + CONTENDED_RWLOCK_WRITERS;
    let ops_per_task = ASYNC_OPS / total_tasks;

    let rwlock = Arc::new(RwLock::new(()));
    let counter = Arc::new(AtomicUsize::new(0));

    for _ in 0..WARMUP {
        counter.store(0, Ordering::SeqCst);
        let mut handles = Vec::new();
        for _ in 0..CONTENDED_RWLOCK_READERS {
            let rwlock = rwlock.clone();
            let counter = counter.clone();
            handles.push(tokio::spawn(async move {
                for _ in 0..ops_per_task {
                    let _guard = rwlock.read().await;
                    counter.fetch_add(1, Ordering::Relaxed);
                }
            }));
        }
        for _ in 0..CONTENDED_RWLOCK_WRITERS {
            let rwlock = rwlock.clone();
            let counter = counter.clone();
            handles.push(tokio::spawn(async move {
                for _ in 0..ops_per_task {
                    let _guard = rwlock.write().await;
                    counter.fetch_add(1, Ordering::Relaxed);
                }
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
    }

    for _ in 0..ITERATIONS {
        reset_alloc_counters();
        counter.store(0, Ordering::SeqCst);
        let start = Instant::now();
        let mut handles = Vec::new();
        for _ in 0..CONTENDED_RWLOCK_READERS {
            let rwlock = rwlock.clone();
            let counter = counter.clone();
            handles.push(tokio::spawn(async move {
                for _ in 0..ops_per_task {
                    let _guard = rwlock.read().await;
                    counter.fetch_add(1, Ordering::Relaxed);
                }
            }));
        }
        for _ in 0..CONTENDED_RWLOCK_WRITERS {
            let rwlock = rwlock.clone();
            let counter = counter.clone();
            handles.push(tokio::spawn(async move {
                for _ in 0..ops_per_task {
                    let _guard = rwlock.write().await;
                    counter.fetch_add(1, Ordering::Relaxed);
                }
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        stats.add(start.elapsed().as_nanos() as u64);
    }

    BenchResult {
        stats,
        ops_per_iter: ASYNC_OPS,
        total_bytes: get_alloc_bytes(),
        total_allocs: get_alloc_count(),
    }
}

// Contended Semaphore: many tasks, few permits, via .await
async fn bench_semaphore_contended() -> BenchResult {
    let mut stats = Stats::new();
    let ops_per_task = ASYNC_OPS / CONTENDED_SEM_TASKS;

    let sem = Arc::new(Semaphore::new(CONTENDED_SEM_PERMITS));
    let counter = Arc::new(AtomicUsize::new(0));

    for _ in 0..WARMUP {
        counter.store(0, Ordering::SeqCst);
        let mut handles = Vec::new();
        for _ in 0..CONTENDED_SEM_TASKS {
            let sem = sem.clone();
            let counter = counter.clone();
            handles.push(tokio::spawn(async move {
                for _ in 0..ops_per_task {
                    let _permit = sem.acquire().await.unwrap();
                    counter.fetch_add(1, Ordering::Relaxed);
                }
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
    }

    for _ in 0..ITERATIONS {
        reset_alloc_counters();
        counter.store(0, Ordering::SeqCst);
        let start = Instant::now();
        let mut handles = Vec::new();
        for _ in 0..CONTENDED_SEM_TASKS {
            let sem = sem.clone();
            let counter = counter.clone();
            handles.push(tokio::spawn(async move {
                for _ in 0..ops_per_task {
                    let _permit = sem.acquire().await.unwrap();
                    counter.fetch_add(1, Ordering::Relaxed);
                }
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        stats.add(start.elapsed().as_nanos() as u64);
    }

    BenchResult {
        stats,
        ops_per_iter: ASYNC_OPS,
        total_bytes: get_alloc_bytes(),
        total_allocs: get_alloc_count(),
    }
}

// ============================================================================
// TIER 4: TASK SCHEDULING (scheduler overhead measurement)
// ============================================================================

/// Serial spawn+await: measures full round-trip through the scheduler.
/// Equivalent to Volt: for _ in 0..N { var f = io.@"async"(noop, .{}); _ = f.@"await"(io); }
async fn bench_task_spawn() -> BenchResult {
    let mut stats = Stats::new();

    for _ in 0..WARMUP {
        for _ in 0..ASYNC_OPS {
            tokio::spawn(async {}).await.unwrap();
        }
    }

    for _ in 0..ITERATIONS {
        reset_alloc_counters();
        let start = Instant::now();
        for _ in 0..ASYNC_OPS {
            tokio::spawn(async {}).await.unwrap();
        }
        stats.add(start.elapsed().as_nanos() as u64);
    }

    BenchResult {
        stats,
        ops_per_iter: ASYNC_OPS,
        total_bytes: get_alloc_bytes(),
        total_allocs: get_alloc_count(),
    }
}

/// Batch spawn: spawn BATCH_SIZE tasks, then await all. Measures scheduler throughput.
/// Equivalent to Volt: spawn 100 tasks via io.@"async", then await all.
async fn bench_task_spawn_batch() -> BenchResult {
    let mut stats = Stats::new();
    let batches = ASYNC_OPS / BATCH_SIZE;

    for _ in 0..WARMUP {
        for _ in 0..batches {
            let mut handles = Vec::with_capacity(BATCH_SIZE);
            for _ in 0..BATCH_SIZE {
                handles.push(tokio::spawn(async {}));
            }
            for h in handles {
                h.await.unwrap();
            }
        }
    }

    for _ in 0..ITERATIONS {
        reset_alloc_counters();
        let start = Instant::now();
        for _ in 0..batches {
            let mut handles = Vec::with_capacity(BATCH_SIZE);
            for _ in 0..BATCH_SIZE {
                handles.push(tokio::spawn(async {}));
            }
            for h in handles {
                h.await.unwrap();
            }
        }
        stats.add(start.elapsed().as_nanos() as u64);
    }

    BenchResult {
        stats,
        ops_per_iter: ASYNC_OPS,
        total_bytes: get_alloc_bytes(),
        total_allocs: get_alloc_count(),
    }
}

/// Blocking pool: spawn+wait via spawn_blocking (dedicated blocking thread).
/// Equivalent to Volt: io.concurrent(noop, .{}) + h.wait()
async fn bench_blocking_spawn() -> BenchResult {
    let mut stats = Stats::new();

    for _ in 0..WARMUP {
        for _ in 0..ASYNC_OPS {
            tokio::task::spawn_blocking(|| {}).await.unwrap();
        }
    }

    for _ in 0..ITERATIONS {
        reset_alloc_counters();
        let start = Instant::now();
        for _ in 0..ASYNC_OPS {
            tokio::task::spawn_blocking(|| {}).await.unwrap();
        }
        stats.add(start.elapsed().as_nanos() as u64);
    }

    BenchResult {
        stats,
        ops_per_iter: ASYNC_OPS,
        total_bytes: get_alloc_bytes(),
        total_allocs: get_alloc_count(),
    }
}

// ============================================================================
// OUTPUT
// ============================================================================

fn print_json_entry(name: &str, result: &BenchResult, is_last: bool) {
    println!("    \"{}\": {{", name);
    println!("      \"ns_per_op\": {:.2},", result.ns_per_op());
    println!("      \"ops_per_sec\": {:.0},", result.ops_per_sec());
    println!("      \"median_ns\": {},", result.stats.median_ns());
    println!("      \"min_ns\": {},", result.stats.min_ns());
    println!("      \"ops_per_iter\": {},", result.ops_per_iter);
    println!("      \"bytes_per_op\": {:.2},", result.bytes_per_op());
    println!("      \"allocs_per_op\": {:.4}", result.allocs_per_op());
    if is_last {
        println!("    }}");
    } else {
        println!("    }},");
    }
}

fn print_row(name: &str, result: &BenchResult) {
    println!(
        "  {:<36} {:>8.1} ns/op  {:>16.0} ops/sec",
        name,
        result.ns_per_op(),
        result.ops_per_sec()
    );
}

// ============================================================================
// MAIN
// ============================================================================

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let json_mode = args.iter().any(|a| a == "--json");

    // ── Tier 1: Sync fast path (no runtime) ──
    let mutex = bench_mutex();
    let rwlock_read = bench_rwlock_read();
    let rwlock_write = bench_rwlock_write();
    let semaphore = bench_semaphore();
    let oncecell_get = bench_oncecell_get();
    let oncecell_set = bench_oncecell_set();

    // ── Tier 2: Channel fast path (no runtime) ──
    let channel_send = bench_channel_send();
    let channel_recv = bench_channel_recv();
    let channel_roundtrip = bench_channel_roundtrip();
    let oneshot_result = bench_oneshot();
    let broadcast_result = bench_broadcast();
    let watch_result = bench_watch();

    // ── Tier 1 + 3: Needs Tokio runtime ──
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(NUM_WORKERS)
        .enable_all()
        .build()
        .unwrap();

    // Tier 1 (need async context but complete immediately)
    let notify = rt.block_on(bench_notify());
    let barrier = rt.block_on(bench_barrier());

    // Tier 3: Async multi-task
    let channel_mpmc = rt.block_on(bench_channel_mpmc());
    let mutex_contended = rt.block_on(bench_mutex_contended());
    let rwlock_contended = rt.block_on(bench_rwlock_contended());
    let semaphore_contended = rt.block_on(bench_semaphore_contended());

    // Tier 4: Task scheduling
    let task_spawn = rt.block_on(bench_task_spawn());
    let task_spawn_batch = rt.block_on(bench_task_spawn_batch());
    let blocking_spawn = rt.block_on(bench_blocking_spawn());

    if json_mode {
        println!("{{");
        println!("  \"metadata\": {{");
        println!("    \"runtime\": \"tokio\",");
        println!("    \"sync_ops\": {},", SYNC_OPS);
        println!("    \"channel_ops\": {},", CHANNEL_OPS);
        println!("    \"async_ops\": {},", ASYNC_OPS);
        println!("    \"iterations\": {},", ITERATIONS);
        println!("    \"warmup\": {},", WARMUP);
        println!("    \"workers\": {},", NUM_WORKERS);
        println!("    \"mpmc_buffer\": {},", MPMC_BUFFER);
        println!(
            "    \"methodology\": \"sync: try_*, channels: try_send/try_recv, async: tokio::spawn + .await\""
        );
        println!("  }},");
        println!("  \"benchmarks\": {{");

        print_json_entry("mutex", &mutex, false);
        print_json_entry("rwlock_read", &rwlock_read, false);
        print_json_entry("rwlock_write", &rwlock_write, false);
        print_json_entry("semaphore", &semaphore, false);
        print_json_entry("oncecell_get", &oncecell_get, false);
        print_json_entry("oncecell_set", &oncecell_set, false);
        print_json_entry("notify", &notify, false);
        print_json_entry("barrier", &barrier, false);
        print_json_entry("channel_send", &channel_send, false);
        print_json_entry("channel_recv", &channel_recv, false);
        print_json_entry("channel_roundtrip", &channel_roundtrip, false);
        print_json_entry("oneshot", &oneshot_result, false);
        print_json_entry("broadcast", &broadcast_result, false);
        print_json_entry("watch", &watch_result, false);
        print_json_entry("channel_mpmc", &channel_mpmc, false);
        print_json_entry("mutex_contended", &mutex_contended, false);
        print_json_entry("rwlock_contended", &rwlock_contended, false);
        print_json_entry("semaphore_contended", &semaphore_contended, false);
        print_json_entry("task_spawn", &task_spawn, false);
        print_json_entry("task_spawn_batch", &task_spawn_batch, false);
        print_json_entry("blocking_spawn", &blocking_spawn, true);

        println!("  }}");
        println!("}}");
    } else {
        println!();
        println!("{}", "=".repeat(74));
        println!("  Tokio Benchmark Suite");
        println!("{}", "=".repeat(74));
        println!(
            "  Tiers: sync={}  channels={}  async={}",
            SYNC_OPS, CHANNEL_OPS, ASYNC_OPS
        );
        println!(
            "  Config: {} iters, {} warmup, {} workers",
            ITERATIONS, WARMUP, NUM_WORKERS
        );
        println!(
            "  Methodology: try_* (sync), try_send/recv (channels), spawn+await (async)"
        );
        println!();

        println!(
            "  TIER 1: SYNC FAST PATH ({} ops, single-thread, try_*)",
            SYNC_OPS
        );
        println!("  {}", "-".repeat(72));
        print_row("Mutex try_lock/drop", &mutex);
        print_row("RwLock try_read/drop", &rwlock_read);
        print_row("RwLock try_write/drop", &rwlock_write);
        print_row("Semaphore try_acquire/drop", &semaphore);
        print_row("OnceCell get (initialized)", &oncecell_get);
        print_row("OnceCell set (new each time)", &oncecell_set);
        print_row("Notify signal+consume*", &notify);
        print_row("Barrier wait (1 participant)*", &barrier);
        println!("  * Tokio requires async for Notify/Barrier (no sync API)");
        println!();

        println!(
            "  TIER 2: CHANNELS ({} ops, single-thread, try_send/try_recv)",
            CHANNEL_OPS
        );
        println!("  {}", "-".repeat(72));
        print_row("Channel send (fill buffer)", &channel_send);
        print_row("Channel recv (drain buffer)", &channel_recv);
        print_row("Channel roundtrip (cap=1)", &channel_roundtrip);
        print_row("Oneshot create+send+recv", &oneshot_result);
        print_row("Broadcast 1tx+4rx", &broadcast_result);
        print_row("Watch send+borrow", &watch_result);
        println!();

        println!(
            "  TIER 3: ASYNC ({} ops, {} workers, spawn+await)",
            ASYNC_OPS, NUM_WORKERS
        );
        println!("  {}", "-".repeat(72));
        print_row(
            &format!("MPMC {}P+{}C (buf={})", MPMC_PRODUCERS, MPMC_CONSUMERS, MPMC_BUFFER),
            &channel_mpmc,
        );
        print_row(
            &format!("Mutex contended ({} tasks)", CONTENDED_MUTEX_TASKS),
            &mutex_contended,
        );
        print_row(
            &format!(
                "RwLock contended ({}R+{}W)",
                CONTENDED_RWLOCK_READERS, CONTENDED_RWLOCK_WRITERS
            ),
            &rwlock_contended,
        );
        print_row(
            &format!(
                "Semaphore contended ({}T, {} permits)",
                CONTENDED_SEM_TASKS, CONTENDED_SEM_PERMITS
            ),
            &semaphore_contended,
        );
        println!();

        println!(
            "  TIER 4: TASK SCHEDULING ({} ops, {} workers)",
            ASYNC_OPS, NUM_WORKERS
        );
        println!("  {}", "-".repeat(72));
        print_row("Spawn+await (noop)", &task_spawn);
        print_row("Spawn batch (100) + await all", &task_spawn_batch);
        print_row("Blocking spawn+wait (noop)", &blocking_spawn);
        println!();

        println!("{}", "=".repeat(74));
        println!("  Use --json for machine-readable output.");
        println!("{}", "=".repeat(74));
        println!();
    }
}
