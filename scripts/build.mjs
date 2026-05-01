import { copyFile, mkdir, readFile, writeFile } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { build, mergeConfig } from 'vite';
import baseConfig from '../vite.config.js';

const workspaceRoot = fileURLToPath(new URL('..', import.meta.url));
const outDir = resolve(workspaceRoot, 'dist');
const manifestPath = resolve(outDir, '.vite', 'manifest.json');
const stableEntries = new Map([
  ['app', 'app.js'],
  ['runtime.worker', 'runtime.worker.js'],
  ['relay.worker', 'relay.worker.js'],
  ['sw', 'sw.js'],
]);

await build(
  mergeConfig(baseConfig, {
    build: {
      manifest: true,
      rollupOptions: {
        input: {
          app: 'app.js',
          sw: 'sw.js',
          'runtime.worker': 'runtime.worker.js',
          'relay.worker': 'relay.worker.js',
        },
        output: {
          entryFileNames: (chunkInfo) => stableEntries.get(chunkInfo.name) ?? 'assets/[name]-[hash].js',
          chunkFileNames: 'assets/[name]-[hash].js',
          assetFileNames: 'assets/[name]-[hash][extname]',
        },
      },
    },
  }),
);

await mkdir(outDir, { recursive: true });
await copyFile(resolve(workspaceRoot, 'app.css'), resolve(outDir, 'app.css'));

const manifest = JSON.parse(await readFile(manifestPath, 'utf8'));
const entry =
  manifest['app.js'] ||
  Object.values(manifest).find((value) => value && typeof value === 'object' && value.isEntry && value.file === 'app.js');

const sourceHtml = await readFile(resolve(workspaceRoot, 'index.html'), 'utf8');
const cssFiles = Array.isArray(entry?.css) ? entry.css : [];
const cssLinks = cssFiles
  .map((file) => `  <link rel="stylesheet" href="./${file}" />`)
  .join('\n');

const builtHtml = sourceHtml.replace(
  '</head>',
  `${cssLinks ? `${cssLinks}\n` : ''}</head>`,
);

await writeFile(resolve(outDir, 'index.html'), builtHtml, 'utf8');
