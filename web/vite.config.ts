import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

// 后端 Rust HTTP 网关默认监听 http://localhost:7878
// 开发期间通过 Vite 代理转发，避免 CORS 问题
export default defineConfig({
  plugins: [react()],
  server: {
    host: '0.0.0.0',
    port: 5173,
    proxy: {
      '/api': 'http://localhost:7878',
      '/ws': {
        target: 'ws://localhost:7878',
        ws: true,
      },
    },
  },
  build: {
    outDir: 'dist',
    sourcemap: true,
  },
});
