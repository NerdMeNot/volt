/** Shared sidebar config — used by astro.config.mjs and scripts/snapshot.mjs.
 *
 * Diátaxis-aligned IA (https://diataxis.fr/):
 *   Getting Started  → tutorials   (learning-oriented)
 *   API Reference    → reference   (information-oriented)
 *   Recipes          → how-to      (task-oriented, copy-pasteable)
 *   Guides           → how-to      (strategy-oriented)
 *   Architecture     → explanation (understanding-oriented)
 *   Performance      → explanation (receipts + postmortems)
 *   Appendix         → reference   (comparisons, glossary, roadmap)
 *
 * URLs are preserved across the 2026-05-16 reorganisation via redirects
 * in astro.config.mjs — never break an inbound link.
 */
export default [
	{ label: 'Introduction', slug: 'index' },
	{
		label: 'Getting Started',
		items: [
			{ label: 'Installation', slug: 'getting-started/installation' },
			{ label: 'Your first program', slug: 'getting-started/first-program' },
			{ label: 'Spawning and joining', slug: 'getting-started/spawn-join' },
			{ label: 'Talk to the network', slug: 'getting-started/io-tutorial' },
			{ label: 'Basic concepts', slug: 'getting-started/basic-concepts' },
			{ label: 'Glossary', slug: 'getting-started/glossary' },
		],
	},
	{
		label: 'API Reference',
		items: [
			{ label: 'The Runtime', slug: 'usage/runtime' },
			{ label: 'Spawning', slug: 'usage/spawning' },
			{ label: 'Structured Concurrency', slug: 'usage/structured-concurrency' },
			{ label: 'Channels', slug: 'usage/channels' },
			{ label: 'Sync Primitives', slug: 'usage/sync' },
			{ label: 'Time', slug: 'usage/time' },
			{ label: 'Networking', slug: 'usage/networking' },
		],
	},
	{
		label: 'Recipes',
		items: [
			{ label: 'Overview', slug: 'cookbook' },
			{ label: 'TCP Echo Server', slug: 'cookbook/echo-server' },
			{ label: 'Fan Out, Take First', slug: 'cookbook/fan-out-first-wins' },
			{ label: 'Offloading CPU Work', slug: 'cookbook/work-offload' },
			{ label: 'Timeout with Retry', slug: 'cookbook/timeout-retry' },
			{ label: 'Graceful Drain', slug: 'cookbook/graceful-drain' },
			{ label: 'Rate Limiter', slug: 'cookbook/rate-limiter' },
			{ label: 'Pub/Sub Fan-Out', slug: 'cookbook/pub-sub' },
			{ label: 'Config Hot-Reload', slug: 'cookbook/config-hot-reload' },
			{ label: 'Connection Pool', slug: 'cookbook/connection-pool' },
		],
	},
	{
		label: 'Guides',
		items: [
			{ label: 'Choosing a Primitive', slug: 'guides/choosing-primitive' },
			{ label: 'Common Pitfalls', slug: 'guides/common-pitfalls' },
			{ label: 'Error Handling', slug: 'guides/error-handling' },
			{ label: 'Performance Tuning', slug: 'guides/performance-tuning' },
		],
	},
	{
		label: 'Architecture',
		items: [
			{ label: 'Overview', slug: 'architecture' },
			{ label: 'Stackful by design', slug: 'architecture/stackful-design' },
			{ label: 'The M:N scheduler', slug: 'architecture/mn-scheduler' },
			{ label: 'Work stealing', slug: 'architecture/work-stealing' },
			{ label: 'Chase-Lev deque', slug: 'architecture/chase-lev-deque' },
			{ label: 'The parking lot', slug: 'architecture/parking-lot' },
			{ label: 'The Parker', slug: 'architecture/parker' },
			{ label: 'Direct handoff', slug: 'architecture/direct-handoff' },
			{ label: 'The slab arena', slug: 'architecture/slab-arena' },
			{ label: 'Stack growth on demand', slug: 'architecture/stack-growth' },
			{ label: 'The kqueue reactor', slug: 'architecture/reactor' },
			{ label: 'The context switch', slug: 'architecture/context-switch' },
			{ label: 'Memory model', slug: 'architecture/memory-model' },
			{ label: 'Cancellation internals', slug: 'architecture/cancellation-internals' },
			{ label: 'Channels internals', slug: 'architecture/channels-internals' },
			{ label: 'Vyukov MPMC (algorithm)', slug: 'architecture/vyukov-mpmc' },
			{ label: 'Semaphore (FIFO algorithm)', slug: 'architecture/semaphore-algorithm' },
		],
	},
	{
		label: 'Performance',
		items: [
			{ label: 'Overview', slug: 'performance' },
			{ label: 'Benchmarks', slug: 'performance/benchmarks' },
			{ label: 'Multi-worker profile', slug: 'performance/multi-worker-profile' },
			{ label: 'Phase 4 postmortem', slug: 'performance/phase-4-postmortem' },
			{ label: 'Slab arena postmortem', slug: 'performance/slab-arena-postmortem' },
		],
	},
	{
		label: 'Testing',
		items: [
			{ label: 'Running Tests', slug: 'testing/running-tests' },
			{ label: 'Benchmarking', slug: 'testing/benchmarking' },
			{ label: 'Writing Async Tests', slug: 'testing/writing-async-tests' },
		],
	},
	{
		label: 'Appendix',
		items: [
			{ label: 'Overview', slug: 'appendix' },
			{ label: 'Roadmap', slug: 'appendix/roadmap' },
			{ label: 'Tokio + Go comparison', slug: 'appendix/tokio-go-comparison' },
			{ label: 'Zig 0.16 notes', slug: 'appendix/zig-016-notes' },
			{ label: 'Contributing', slug: 'appendix/contributing' },
		],
	},
];
