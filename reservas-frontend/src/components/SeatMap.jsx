const STATUS_STYLES = {
  AVAILABLE:
    'bg-slate-100 dark:bg-slate-800 border-slate-300 dark:border-slate-600 hover:border-emerald-500 hover:bg-emerald-50 dark:hover:bg-emerald-500/10 cursor-pointer',
  SELECTED:
    'bg-sky-100 dark:bg-sky-500/20 border-sky-500 border-2 text-sky-700 dark:text-sky-300 cursor-pointer ring-2 ring-sky-400/50',
  HELD:
    'bg-amber-100 dark:bg-amber-500/20 border-amber-500 text-amber-700 dark:text-amber-300 cursor-not-allowed',
  CONFIRMED:
    'bg-slate-200 dark:bg-slate-700 border-slate-300 dark:border-slate-700 text-slate-400 dark:text-slate-500 cursor-not-allowed',
}

const STATUS_LABELS = {
  AVAILABLE: 'Disponible',
  SELECTED: 'Seleccionado',
  HELD: 'Reservado por otra persona',
  CONFIRMED: 'Confirmado',
}

function groupByRow(seats) {
  const rows = {}
  for (const seat of seats) {
    if (!rows[seat.row]) rows[seat.row] = []
    rows[seat.row].push(seat)
  }
  return Object.entries(rows).sort(([a], [b]) => a.localeCompare(b))
}

function SeatMap({ seats, selectedSeatId, onSeatClick }) {
  const rows = groupByRow(seats)

  return (
    <div className="space-y-6">
      {/* Barra "escenario" para dar contexto espacial, como en un cine real */}
      <div className="flex justify-center">
        <div className="w-2/3 h-2 rounded-full bg-gradient-to-r from-transparent via-slate-400 dark:via-slate-500 to-transparent" />
      </div>
      <p className="text-center text-xs uppercase tracking-widest text-slate-400 dark:text-slate-500">
        Escenario
      </p>

      <div className="space-y-3 pt-2">
        {rows.map(([row, rowSeats]) => (
          <div key={row} className="flex items-center gap-3">
            <span className="w-6 text-sm font-mono text-slate-500">
              {row}
            </span>
            <div className="flex gap-2 flex-wrap">
              {rowSeats
                .sort((a, b) => a.number - b.number)
                .map((seat) => {
                  const isSelected = seat.id === selectedSeatId
                  const visualStatus = isSelected ? 'SELECTED' : seat.status
                  return (
                    <button
                      key={seat.id}
                      disabled={seat.status !== 'AVAILABLE'}
                      onClick={() => onSeatClick?.(seat)}
                      className={`w-10 h-10 rounded-md border text-sm font-medium transition-colors ${STATUS_STYLES[visualStatus]}`}
                      title={`Fila ${row}, asiento ${seat.number} — ${STATUS_LABELS[visualStatus]}`}
                    >
                      {seat.number}
                    </button>
                  )
                })}
            </div>
          </div>
        ))}
      </div>

      <div className="flex flex-wrap gap-6 text-xs text-slate-500 dark:text-slate-400 pt-4 border-t border-slate-200 dark:border-slate-800">
        <span className="flex items-center gap-2">
          <span className="w-4 h-4 rounded border border-slate-300 dark:border-slate-600 bg-slate-100 dark:bg-slate-800 inline-block" />
          Disponible
        </span>
        <span className="flex items-center gap-2">
          <span className="w-4 h-4 rounded border-2 border-sky-500 bg-sky-100 dark:bg-sky-500/20 inline-block" />
          Seleccionado
        </span>
        <span className="flex items-center gap-2">
          <span className="w-4 h-4 rounded border border-amber-500 bg-amber-100 dark:bg-amber-500/20 inline-block" />
          Reservado por otra persona
        </span>
        <span className="flex items-center gap-2">
          <span className="w-4 h-4 rounded border border-slate-300 dark:border-slate-700 bg-slate-200 dark:bg-slate-700 inline-block" />
          Confirmado
        </span>
      </div>
    </div>
  )
}

export default SeatMap
