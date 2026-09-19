import { Link } from 'react-router-dom'
import { mockShows } from '../mock/shows'

function formatDate(isoString) {
  const date = new Date(isoString)
  return date.toLocaleString('es-AR', {
    dateStyle: 'full',
    timeStyle: 'short',
  })
}

function ShowSelectorPage() {
  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold">Elegí una función</h1>
        <p className="text-slate-500 dark:text-slate-400 text-sm mt-1">
          Seleccioná la función para ver el mapa de asientos disponibles.
        </p>
      </div>

      <div className="grid gap-4 sm:grid-cols-2">
        {mockShows.map((show) => (
          <Link
            key={show.id}
            to={`/show/${show.id}`}
            className="block rounded-lg border border-slate-200 dark:border-slate-800 bg-slate-50 dark:bg-slate-800/40 p-5 hover:border-emerald-500 hover:bg-emerald-50 dark:hover:bg-slate-800 transition-colors"
          >
            <h2 className="font-semibold text-lg">{show.title}</h2>
            <p className="text-slate-500 dark:text-slate-400 text-sm mt-1">{show.venue}</p>
            <p className="text-emerald-600 dark:text-emerald-400 text-sm mt-3">
              {formatDate(show.date)}
            </p>
          </Link>
        ))}
      </div>
    </div>
  )
}

export default ShowSelectorPage
