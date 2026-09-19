package com.portfolio.reservas.event;

import java.time.Instant;
import java.util.UUID;

public record SeatReservationRequestEvent(
        UUID eventId,
        UUID showId,
        UUID seatId,
        String userId,
        String action,
        Instant timestamp
) {
    public static SeatReservationRequestEvent of(UUID showId, UUID seatId, String userId) {
        return new SeatReservationRequestEvent(
                UUID.randomUUID(),
                showId,
                seatId,
                userId,
                "REQUEST_HOLD",
                Instant.now()
        );
    }
}
