import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  server: {
    port: 3228,
    proxy: {
      '/api': {
        target: 'http://localhost:8600',
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/api/, ''),
      },
      '/ws': {
        target: 'ws://localhost:8600',
        ws: true,
      },
    },
  },
  build: {
    outDir: 'dist',
    sourcemap: true,
    rollupOptions: {
      output: {
        manualChunks(id) {
          if (!id.includes('node_modules')) {
            return
          }
          if (id.includes('react-router-dom') || id.includes('react-dom') || id.includes('/react/')) {
            return 'react-vendor'
          }
          if (id.includes('@tanstack/react-query')) {
            return 'query-vendor'
          }
          if (id.includes('axios')) {
            return 'network-vendor'
          }
          if (id.includes('zustand')) {
            return 'state-vendor'
          }
          if (id.includes('echarts') || id.includes('recharts')) {
            return 'charts-vendor'
          }
          if (id.includes('@ant-design/icons')) {
            return 'icons-vendor'
          }
        },
      },
    },
    chunkSizeWarningLimit: 950,
  },
})
