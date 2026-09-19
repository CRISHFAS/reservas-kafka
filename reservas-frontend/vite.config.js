import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'
import { defineConfig } from 'vite'
// https://vite.dev/config/
export default defineConfig({
  plugins: [react(), tailwindcss()],
  server: {
    host: '0.0.0.0',
    port: 3000,
  },
  define: {
    // sockjs-client asume un entorno Node donde existe `global`; el
    // navegador no lo tiene, y sin esto rompe con
    // "ReferenceError: global is not defined" al cargar la app.
    global: 'window',
  },
})
