// Datos mock para diseñar la interacción del mapa de asientos.
// Los IDs coinciden con los Seat reales de prueba ya cargados en el
// backend (ver HANDOFF-proyecto-reservas.md), para que al conectar datos
// reales en el próximo paso no haya que inventar nada nuevo.
export const mockSeats = [
  {
    id: 'b1b1b1b1-1111-1111-1111-111111111111',
    row: 'A',
    number: 1,
    status: 'AVAILABLE',
  },
  {
    id: 'b1b1b1b1-2222-2222-2222-222222222222',
    row: 'A',
    number: 2,
    status: 'HELD',
  },
  {
    id: 'b1b1b1b1-3333-3333-3333-333333333333',
    row: 'A',
    number: 3,
    status: 'CONFIRMED',
  },
]
