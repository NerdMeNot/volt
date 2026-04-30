/** Shared sidebar config — used by astro.config.mjs and scripts/snapshot.mjs. */
export default [
	{ label: 'Introduction', slug: 'index' },
	{
		label: 'Getting Started',
		items: [
			{ label: 'Installation', slug: 'getting-started/installation' },
			{ label: 'Quick Start', slug: 'getting-started/quick-start' },
			{ label: 'Basic Concepts', slug: 'getting-started/basic-concepts' },
			{ label: 'Glossary', slug: 'getting-started/glossary' },
		],
	},
	{
		label: 'Usage',
		items: [
			{ label: 'The Runtime', slug: 'usage/runtime' },
			{ label: 'Spawning', slug: 'usage/spawning' },
			{ label: 'Structured Concurrency', slug: 'usage/structured-concurrency' },
			{ label: 'Channels', slug: 'usage/channels' },
			{ label: 'Sync Primitives', slug: 'usage/sync' },
			{ label: 'Time', slug: 'usage/time' },
			{ label: 'Networking', slug: 'usage/networking' },
			{ label: 'Signals & Shutdown', slug: 'usage/signals-shutdown' },
			{ label: 'Subprocess Management', slug: 'usage/process' },
			{ label: 'Filesystem', slug: 'usage/filesystem' },
			{ label: 'Observability', slug: 'usage/observability' },
		],
	},
	{
		label: 'Cookbook',
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
		label: 'Design',
		items: [
			{ label: 'Stackless vs Stackful', slug: 'design/stackless-vs-stackful' },
		],
	},
	{
		label: 'Algorithms',
		items: [
			{ label: 'Work Stealing', slug: 'algorithms/work-stealing' },
			{ label: 'Chase-Lev Deque', slug: 'algorithms/chase-lev-deque' },
			{ label: 'Vyukov MPMC Queue', slug: 'algorithms/vyukov-mpmc' },
			{ label: 'Semaphore (FIFO)', slug: 'algorithms/semaphore-algorithm' },
			{ label: 'Park', slug: 'algorithms/park' },
		],
	},
	{
		label: 'Internals',
		items: [
			{ label: 'Architecture', slug: 'internals/architecture' },
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
];
