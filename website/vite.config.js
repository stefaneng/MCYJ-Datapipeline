import { defineConfig } from 'vite'
import { resolve } from 'path'

// Use VITE_BASE_URL environment variable if set, otherwise default to '/'
// For GitHub Pages, set VITE_BASE_URL=/MCYJ-Datapipeline/ in the build environment
// For Netlify, the default '/' works correctly
const base = process.env.VITE_BASE_URL || '/'

export default defineConfig({
  base,
  build: {
    outDir: '../dist',
    emptyOutDir: true,
    rollupOptions: {
      input: {
        main: resolve(__dirname, 'index.html'),
        keywords: resolve(__dirname, 'keywords.html'),
        document: resolve(__dirname, 'document.html'),
        facilities: resolve(__dirname, 'facilities.html')
      }
    }
  },
  publicDir: 'public'
})
