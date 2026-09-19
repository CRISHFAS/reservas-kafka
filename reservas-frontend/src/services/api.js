import { Client } from '@stomp/stompjs'
import SockJS from 'sockjs-client'

const API_BASE_URL = `http://${window.location.hostname}:8080/api`
const WS_URL = `http://${window.location.hostname}:8080/ws`

export async function fetchSeats(showId) {
  const response = await fetch(`${API_BASE_URL}/shows/${showId}/seats`)
  if (!response.ok) {
    throw new Error(`Error al obtener asientos: ${response.status}`)
  }
  return response.json()
}

export async function postReservation({ showId, seatId, userId }) {
  const response = await fetch(`${API_BASE_URL}/reservations`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ showId, seatId, userId }),
  })
  if (!response.ok) {
    throw new Error(`Error al reservar: ${response.status}`)
  }
  return response.text()
}

export async function confirmPurchase({ showId, seatId, userId }) {
  const response = await fetch(`${API_BASE_URL}/reservations/confirm`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ showId, seatId, userId }),
  })
  const text = await response.text()
  if (!response.ok) {
    throw new Error(text || `Error al confirmar: ${response.status}`)
  }
  return text
}

export function subscribeToShow(showId, onEvent) {
  const client = new Client({
    webSocketFactory: () => new SockJS(WS_URL),
    reconnectDelay: 5000,
    onConnect: () => {
      client.subscribe(`/topic/shows/${showId}`, (message) => {
        onEvent(JSON.parse(message.body))
      })
    },
  })
  client.activate()

  return () => {
    client.deactivate()
  }
}
