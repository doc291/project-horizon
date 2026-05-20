import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

// Horizon V1 frontend — M1 build + test config.
// Static React build; no proxy, no dev API, no backend coupling.
// Vitest integrated via test config below (M1 §5 + §17).
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
  test: {
    environment: 'node',
    include: ['tests/**/*.test.{js,jsx}'],
    globals: false,
  },
});
