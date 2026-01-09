import { defineConfig } from 'vite'
import { resolve } from 'path'
import react from '@vitejs/plugin-react'

// Use VITE_BASE_URL environment variable if set, otherwise default to '/'
// For GitHub Pages, set VITE_BASE_URL=/MCYJ-Datapipeline/ in the build environment
// For Netlify, the default '/' works correctly
const base = process.env.VITE_BASE_URL || '/'

// Get commit hash from environment variable
// - VITE_COMMIT_HASH: set by GitHub Actions workflow
// - COMMIT_REF: provided by Netlify
// - 'dev': fallback for local builds
const commitHash = process.env.VITE_COMMIT_HASH || process.env.COMMIT_REF || 'dev'

export default defineConfig({
  base,
  plugins: [react()],
  define: {
    __COMMIT_HASH__: JSON.stringify(commitHash)
  },
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
