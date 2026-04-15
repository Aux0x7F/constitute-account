import { copyFile, mkdir } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { build, mergeConfig } from 'vite';
import baseConfig from '../vite.config.js';

const workspaceRoot = fileURLToPath(new URL('..', import.meta.url));
const outDir = resolve(workspaceRoot, 'dist');
const stableEntries = new Map([
  ['app', 'app.js'],
  ['runtime.worker', 'runtime.worker.js'],
  ['relay.worker', 'relay.worker.js'],
  ['sw', 'sw.js'],
]);

await build(
  mergeConfig(baseConfig, {
    build: {
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
await copyFile(resolve(workspaceRoot, 'index.html'), resolve(outDir, 'index.html'));
await copyFile(resolve(workspaceRoot, 'app.css'), resolve(outDir, 'app.css'));
