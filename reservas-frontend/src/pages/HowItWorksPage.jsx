import { Link } from 'react-router-dom'

const SECTIONS = [
  {
    title: '1. La reserva no es instantánea, es asíncrona',
    body: 'Al hacer click en un asiento, el frontend no reserva nada directamente: manda un POST que solo encola una "solicitud de reserva" en un tópico de Kafka (seat-reservation-requests). Un consumer del backend la procesa después, en su propio tiempo, y recién ahí decide si el asiento se puede tomar. Por eso la respuesta inmediata del click es solo "procesando...", no un resultado definitivo.',
  },
  {
    title: '2. El resultado llega por WebSocket, no por la respuesta HTTP',
    body: 'El resultado real (HELD si se pudo reservar, REJECTED si no) se publica en un segundo tópico de Kafka (seat-reservation-events) y se retransmite en vivo por WebSocket (STOMP sobre SockJS) a todas las personas conectadas a esa función. Así el mapa de asientos se actualiza para todos en tiempo real, sin que nadie tenga que recargar la página.',
  },
  {
    title: '3. Las carreras entre dos personas se resuelven en el backend, no en el navegador',
    body: 'Si dos personas clickean el mismo asiento casi al mismo tiempo, ambas solicitudes entran a Kafka y se procesan en orden estricto (partición por función/show). La primera en procesarse gana el asiento (HELD); la segunda es rechazada (REJECTED) porque el chequeo de disponibilidad ya lo encuentra ocupado. Un optimistic lock (@Version en la entidad Seat) protege además contra condiciones de carrera a nivel de base de datos.',
  },
  {
    title: '4. Los asientos no confirmados se liberan solos',
    body: 'Un asiento en HELD tiene 5 minutos para confirmarse. Un proceso programado (@Scheduled) corre cada 30 segundos en el backend, detecta los asientos vencidos, los libera, y publica un evento EXPIRED por el mismo canal de Kafka/WebSocket — así el mapa se actualiza solo, sin que nadie tenga que refrescar.',
  },
  {
    title: '5. Cada evento se procesa una sola vez, incluso si Kafka lo reintenta',
    body: 'Kafka puede reentregar un mensaje si el consumer se reinicia en medio del procesamiento. Para no duplicar una reserva por eso, cada evento procesado queda registrado (tabla processed_events) antes de continuar, y el consumer chequea ese registro al principio: si ya lo vio, lo descarta sin volver a aplicarlo.',
  },
]

function HowItWorksPage() {
  return (
    <div className="space-y-8">
      <div>
        <Link to="/" className="text-sm text-slate-500 dark:text-slate-400 hover:text-emerald-600 dark:hover:text-emerald-400">
          &larr; Volver a funciones
        </Link>
        <h1 className="text-2xl font-bold mt-4">Cómo funciona por dentro</h1>
        <p className="text-slate-500 dark:text-slate-400 text-sm mt-1">
          Este sistema de reservas está armado sobre Spring Boot + Apache Kafka
          + WebSocket, pensado para manejar reservas concurrentes sin
          sobrevender ni bloquear a nadie innecesariamente.
        </p>
      </div>

      <div className="space-y-6">
        {SECTIONS.map((section) => (
          <div
            key={section.title}
            className="rounded-lg border border-slate-200 dark:border-slate-800 bg-slate-50 dark:bg-slate-800/40 p-5"
          >
            <h2 className="font-semibold text-emerald-600 dark:text-emerald-400 mb-2">
              {section.title}
            </h2>
            <p className="text-sm text-slate-600 dark:text-slate-300 leading-relaxed">
              {section.body}
            </p>
          </div>
        ))}
      </div>
    </div>
  )
}

export default HowItWorksPage
