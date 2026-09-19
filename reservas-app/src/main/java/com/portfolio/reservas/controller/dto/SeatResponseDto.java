package com.portfolio.reservas.controller.dto;
import com.portfolio.reservas.domain.Seat;
import java.time.LocalDateTime;
import java.util.UUID;
public record SeatResponseDto(
        UUID id,
        String row,
        Integer number,
        String status,
        LocalDateTime heldUntil,
        String heldBy
) {
    public static SeatResponseDto from(Seat seat) {
        return new SeatResponseDto(
                seat.getId(),
                seat.getRow(),
                seat.getNumber(),
                seat.getStatus().name(),
                seat.getHeldUntil(),
                seat.getHeldBy()
        );
    }
}
