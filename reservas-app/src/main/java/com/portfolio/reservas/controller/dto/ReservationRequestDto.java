package com.portfolio.reservas.controller.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.util.UUID;

public record ReservationRequestDto(
        @NotNull UUID showId,
        @NotNull UUID seatId,
        @NotBlank String userId
) {
}
