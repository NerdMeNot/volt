// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import tailwindcss from '@tailwindcss/vite';
import starlightVersions from 'starlight-versions';
import { readFileSync } from 'node:fs';
import sidebar from './sidebar.mjs';

// Read version config
let versionsConfig;
try {
	versionsConfig = JSON.parse(readFileSync(new URL('./versions.json', import.meta.url), 'utf-8'));
} catch {
	versionsConfig = { current: null, history: [] };
}

// Only enable version plugin when there are older versions to show
const versionPlugins = versionsConfig.history.length > 0
	? [starlightVersions({
			current: { label: `${versionsConfig.current} (Latest)` },
			versions: versionsConfig.history.map(v => ({ slug: v, label: v })),
		})]
	: [];

// https://astro.build/config
export default defineConfig({
	site: 'https://volt.nerdmenot.in',
	// Preserve URLs across the 2026-05-16 IA reorganisation. Old paths
	// (algorithms/, design/, internals/) → new locations under
	// architecture/ and performance/. Out-of-scope pages (filesystem,
	// process, signals-shutdown, observability) redirect to the
	// roadmap which explains what lives outside Volt core.
	redirects: {
		// internals/ → architecture/ + performance/
		'/internals/architecture': '/architecture',
		'/internals/scheduler-mn': '/architecture/mn-scheduler',
		'/internals/parking-lot': '/architecture/parking-lot',
		'/internals/memory-model': '/architecture/memory-model',
		'/internals/direct-handoff-design': '/architecture/direct-handoff',
		'/internals/multi-worker-profile': '/performance/multi-worker-profile',
		'/internals/phase-4-postmortem': '/performance/phase-4-postmortem',
		// algorithms/ → architecture/
		'/algorithms/work-stealing': '/architecture/work-stealing',
		'/algorithms/chase-lev-deque': '/architecture/chase-lev-deque',
		'/algorithms/vyukov-mpmc': '/architecture/vyukov-mpmc',
		'/algorithms/semaphore-algorithm': '/architecture/semaphore-algorithm',
		'/algorithms/park': '/architecture/parker',
		// design/ → architecture/
		'/design/stackless-vs-stackful': '/architecture/stackful-design',
		// Out-of-scope (deleted) → roadmap explains where each lives now
		'/usage/filesystem': '/appendix/roadmap',
		'/usage/process': '/appendix/roadmap',
		'/usage/signals-shutdown': '/appendix/roadmap',
		'/usage/observability': '/appendix/roadmap',
		// Quick Start subsumed by the new tutorial track (first-program,
		// spawn-join, io-tutorial). Redirect to the first one.
		'/getting-started/quick-start': '/getting-started/first-program',
	},
	vite: {
		plugins: [tailwindcss()],
	},
	integrations: [
		starlight({
			title: 'Volt',
			logo: {
				light: './src/assets/logo-light.png',
				dark: './src/assets/logo-dark.png',
				replacesTitle: true,
			},
			favicon: '/favicon.png',
			social: [
				{ icon: 'github', label: 'GitHub', href: 'https://github.com/NerdMeNot/volt' },
			],
			plugins: versionPlugins,
			components: {
				ThemeSelect: './src/components/ThemeSelect.astro',
				Head: './src/components/Head.astro',
			},
			customCss: [
				'./src/styles/global.css',
			],
			sidebar,
		}),
	],
});
