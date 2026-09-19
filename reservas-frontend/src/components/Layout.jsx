import { Link } from 'react-router-dom'
import ThemeToggle from './ThemeToggle'

function Layout({ children }) {
  return (
    <div className="min-h-screen bg-white dark:bg-slate-900 text-slate-900 dark:text-slate-100 flex flex-col transition-colors">
      <header className="border-b border-slate-200 dark:border-slate-800 bg-white/50 dark:bg-slate-950/50 backdrop-blur sticky top-0 z-10">
        <div className="max-w-5xl mx-auto px-4 py-4 flex items-center justify-between">
          <Link to="/" className="text-lg font-bold text-emerald-600 dark:text-emerald-400">
            Reservas
          </Link>
          <div className="flex items-center gap-4">
            <Link
              to="/como-funciona"
              className="text-xs text-slate-500 dark:text-slate-400 hover:text-emerald-600 dark:hover:text-emerald-400"
            >
              Cómo funciona por dentro
            </Link>
            <ThemeToggle />
          </div>
        </div>
      </header>
      <main className="flex-1 max-w-5xl w-full mx-auto px-4 py-8">
        {children}
      </main>
      <footer className="border-t border-slate-200 dark:border-slate-800 py-4 text-center text-xs text-slate-500 dark:text-slate-600">
        Proyecto de portfolio — Spring Boot + Kafka + React
      </footer>
    </div>
  )
}

export default Layout
