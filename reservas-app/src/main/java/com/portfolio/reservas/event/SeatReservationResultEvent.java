package com.portfolio.reservas.event;

import java.time.Instant;
import java.util.UUID;

public record SeatReservationResultEvent(
        UUID eventId,
        UUID showId,
        UUID seatId,
        String userId,
        String result,
        String reason,
        Instant timestamp
) {
    public static SeatReservationResultEvent of(UUID showId, UUID seatId, String userId, String result, String reason) {
        return new SeatReservationResultEvent(
                UUID.randomUUID(),
                showId,
                seatId,
                userId,
                result,
                reason,
                Instant.now()
        );
    }
}
