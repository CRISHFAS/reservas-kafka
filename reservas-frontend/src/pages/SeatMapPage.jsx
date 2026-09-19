import { useParams, Link } from 'react-router-dom'
import { useEffect, useRef, useState } from 'react'
import SeatMap from '../components/SeatMap'
import { fetchSeats, postReservation, confirmPurchase, subscribeToShow } from '../services/api'

const USER_ID_STORAGE_KEY = 'reservas-userId'

function resultToSeatStatus(result) {
  if (result === 'HELD') return 'HELD'
  if (result === 'EXPIRED') return 'AVAILABLE'
  if (result === 'CONFIRMED') return 'CONFIRMED'
  return null
}

function humanizeReason(result, reason) {
  if (result === 'HELD') return null
  if (reason?.includes('Conflicto de concurrencia')) {
    return 'Uy, alguien lo reservó un instante antes que vos. Probá con otro asiento.'
  }
  if (reason?.includes('Ya tenes otro asiento')) {
    return 'Ya tenés otro asiento reservado para esta función. Confirmalo o esperá a que venza antes de elegir otro.'
  }
  if (reason?.includes('no disponible')) {
    return 'Ese asiento ya no está disponible. Elegí otro de la lista.'
  }
  return 'No pudimos completar la reserva. Probá de nuevo en unos segundos.'
}

function parseUtcDate(dateString) {
  if (!dateString) return null
  return new Date(dateString.endsWith('Z') ? dateString : dateString + 'Z')
}

function formatCountdown(ms) {
  if (ms <= 0) return '0:00'
  const totalSeconds = Math.floor(ms / 1000)
  const minutes = Math.floor(totalSeconds / 60)
  const seconds = totalSeconds % 60
  return `${minutes}:${seconds.toString().padStart(2, '0')}`
}

function SeatMapPage() {
  const { showId } = useParams()
  const [seats, setSeats] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [userId, setUserId] = useState(
    () => localStorage.getItem(USER_ID_STORAGE_KEY) || ''
  )
  const [selectedSeat, setSelectedSeat] = useState(null)
  const [confirming, setConfirming] = useState(false)
  const [confirmingPurchase, setConfirmingPurchase] = useState(false)
  const [feedback, setFeedback] = useState(null)
  const [now, setNow] = useState(() => Date.now())
  const pendingSeatIdRef = useRef(null)
  const myActiveSeatIdRef = useRef(null)

  useEffect(() => {
    setLoading(true)
    setError(null)
    fetchSeats(showId)
      .then(setSeats)
      .catch((err) => setError(err.message))
      .finally(() => setLoading(false))
  }, [showId])

  useEffect(() => {
    const interval = setInterval(() => setNow(Date.now()), 1000)
    return () => clearInterval(interval)
  }, [])

  useEffect(() => {
    const unsubscribe = subscribeToShow(showId, (event) => {
      const newStatus = resultToSeatStatus(event.result)
      if (newStatus) {
        setSeats((prev) =>
          prev.map((seat) =>
            seat.id === event.seatId
              ? { ...seat, status: newStatus, heldBy: newStatus === 'HELD' ? event.userId : null, heldUntil: newStatus === 'HELD' ? seat.heldUntil : null }
              : seat
          )
        )
      }

      const isMyPendingAction =
        event.userId === userId.trim() &&
        event.seatId === pendingSeatIdRef.current

      if (isMyPendingAction) {
        if (event.result === 'HELD') {
          fetchSeats(showId).then(setSeats).catch(() => {})
          setFeedback({ type: 'success', text: '¡Listo! Reservamos tu asiento por 5 minutos.' })
          setSelectedSeat(null)
          myActiveSeatIdRef.current = event.seatId
        } else {
          setFeedback({ type: 'error', text: humanizeReason(event.result, event.reason) })
          setSelectedSeat(null)
        }
        pendingSeatIdRef.current = null
      }

      if (event.result === 'EXPIRED' && event.seatId === myActiveSeatIdRef.current) {
        setFeedback(null)
        myActiveSeatIdRef.current = null
      }
    })

    return unsubscribe
  }, [showId, userId])

  function handleUserIdChange(event) {
    const value = event.target.value
    setUserId(value)
    localStorage.setItem(USER_ID_STORAGE_KEY, value)
  }

  function handleSeatClick(seat) {
    setFeedback(null)
    setSelectedSeat(seat)
  }

  function handleCancelSelection() {
    setSelectedSeat(null)
    setFeedback(null)
  }

  async function handleConfirmReservation() {
    if (!userId.trim()) {
      setFeedback({ type: 'error', text: 'Ingresá tu nombre antes de confirmar.' })
      return
    }
    if (!selectedSeat) return

    const seatId = selectedSeat.id
    pendingSeatIdRef.current = seatId
    setConfirming(true)
    setFeedback(null)
    try {
      await postReservation({ showId, seatId, userId: userId.trim() })
      // El evento del WebSocket puede llegar ANTES de que esta línea se
      // ejecute (viaja por un canal aparte, más rápido que la respuesta
      // de este POST). Si pendingSeatIdRef ya se limpió, es porque el
      // resultado real ya se procesó — no lo pisamos con este mensaje
      // interino (bug real de condición de carrera encontrado en la
      // verificación de esta fase).
      if (pendingSeatIdRef.current === seatId) {
        setFeedback({ type: 'info', text: 'Confirmando tu reserva...' })
      }
    } catch (err) {
      pendingSeatIdRef.current = null
      setFeedback({ type: 'error', text: 'No pudimos enviar tu solicitud. Probá de nuevo.' })
    } finally {
      setConfirming(false)
    }
  }

  async function handleConfirmPurchase(seat) {
    setConfirmingPurchase(true)
    setFeedback(null)
    try {
      const message = await confirmPurchase({ showId, seatId: seat.id, userId: userId.trim() })
      setSeats((prev) =>
        prev.map((s) => (s.id === seat.id ? { ...s, status: 'CONFIRMED', heldUntil: null } : s))
      )
      setFeedback({ type: 'success', text: message })
      myActiveSeatIdRef.current = null
    } catch (err) {
      setFeedback({ type: 'error', text: err.message })
    } finally {
      setConfirmingPurchase(false)
    }
  }

  const myHeldSeat = seats.find(
    (seat) => seat.status === 'HELD' && seat.heldBy === userId.trim() && seat.heldUntil && parseUtcDate(seat.heldUntil) > new Date(now)
  )

  return (
    <div className="space-y-6">
      <Link to="/" className="text-sm text-slate-500 dark:text-slate-400 hover:text-emerald-600 dark:hover:text-emerald-400">
        &larr; Volver a funciones
      </Link>

      <div>
        <h1 className="text-2xl font-bold">Elegí tu asiento</h1>
        <p className="text-slate-500 dark:text-slate-400 text-sm mt-1">
          Tocá un asiento disponible para seleccionarlo. Una vez que confirmes,
          lo vamos a reservar por 5 minutos para darte tiempo de completar tu compra.
        </p>
      </div>

      <div>
        <label className="block text-sm text-slate-500 dark:text-slate-400 mb-1">
          Tu nombre
        </label>
        <input
          type="text"
          value={userId}
          onChange={handleUserIdChange}
          placeholder="Ej: Juan Pérez"
          className="w-full max-w-xs rounded-md bg-slate-50 dark:bg-slate-800 border border-slate-300 dark:border-slate-700 px-3 py-2 text-sm focus:outline-none focus:border-emerald-500"
        />
      </div>

      {loading && (
        <p className="text-slate-500 dark:text-slate-400">Cargando asientos...</p>
      )}

      {error && (
        <p className="text-red-600 dark:text-red-400">
          No se pudieron cargar los asientos: {error}
        </p>
      )}

      {feedback && (
        <div
          className={`rounded-md px-4 py-3 text-sm ${
            feedback.type === 'error'
              ? 'bg-red-50 dark:bg-red-500/10 text-red-700 dark:text-red-400 border border-red-200 dark:border-red-500/30'
              : feedback.type === 'success'
                ? 'bg-emerald-50 dark:bg-emerald-500/10 text-emerald-700 dark:text-emerald-400 border border-emerald-200 dark:border-emerald-500/30'
                : 'bg-sky-50 dark:bg-sky-500/10 text-sky-700 dark:text-sky-400 border border-sky-200 dark:border-sky-500/30'
          }`}
        >
          {feedback.text}
        </div>
      )}

      {myHeldSeat && (
        <div className="rounded-md px-4 py-3 text-sm bg-amber-50 dark:bg-amber-500/10 text-amber-700 dark:text-amber-400 border border-amber-200 dark:border-amber-500/30 flex items-center justify-between gap-4 flex-wrap">
          <span>
            Tenés el asiento {myHeldSeat.row}{myHeldSeat.number} reservado.
            Te queda <strong>{formatCountdown(parseUtcDate(myHeldSeat.heldUntil) - now)}</strong> para confirmar tu compra.
          </span>
          <button
            onClick={() => handleConfirmPurchase(myHeldSeat)}
            disabled={confirmingPurchase}
            className="text-sm px-4 py-2 rounded-md bg-emerald-600 hover:bg-emerald-500 text-white font-medium disabled:opacity-60 shrink-0"
          >
            {confirmingPurchase ? 'Confirmando...' : 'Confirmar compra'}
          </button>
        </div>
      )}

      {!loading && !error && (
        <SeatMap
          seats={seats}
          selectedSeatId={selectedSeat?.id}
          onSeatClick={handleSeatClick}
        />
      )}

      {selectedSeat && (
        <div className="sticky bottom-4 rounded-lg border border-sky-300 dark:border-sky-700 bg-sky-50 dark:bg-sky-950/60 backdrop-blur px-5 py-4 flex items-center justify-between gap-4 shadow-lg">
          <div>
            <p className="font-semibold">
              Asiento {selectedSeat.row}{selectedSeat.number} seleccionado
            </p>
            <p className="text-xs text-slate-500 dark:text-slate-400">
              Confirmá para reservarlo por 5 minutos.
            </p>
          </div>
          <div className="flex gap-2 shrink-0">
            <button
              onClick={handleCancelSelection}
              className="text-sm px-3 py-2 rounded-md border border-slate-300 dark:border-slate-700 hover:bg-slate-100 dark:hover:bg-slate-800"
            >
              Cancelar
            </button>
            <button
              onClick={handleConfirmReservation}
              disabled={confirming}
              className="text-sm px-4 py-2 rounded-md bg-emerald-600 hover:bg-emerald-500 text-white font-medium disabled:opacity-60"
            >
              {confirming ? 'Confirmando...' : 'Confirmar reserva'}
            </button>
          </div>
        </div>
      )}
    </div>
  )
}

export default SeatMapPage
