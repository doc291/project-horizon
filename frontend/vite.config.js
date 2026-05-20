import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

// Horizon V1 frontend — M0 build config.
// Static React build only; no proxy, no dev API, no backend.
export default defineConfig({
  plugins: [react()],
  build: {
    outDir: 'dist',
    sourcemap: false,
    emptyOutDir: true,
  },
  server: {
    port: 5173,
    strictPort: false,
  },
});
