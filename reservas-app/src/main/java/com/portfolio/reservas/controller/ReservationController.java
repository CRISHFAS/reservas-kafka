package com.portfolio.reservas.controller;

import com.portfolio.reservas.controller.dto.ReservationRequestDto;
import com.portfolio.reservas.service.ConfirmationResult;
import com.portfolio.reservas.service.ReservationConfirmationService;
import com.portfolio.reservas.service.ReservationProducerService;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/reservations")
public class ReservationController {

    private final ReservationProducerService producerService;
    private final ReservationConfirmationService confirmationService;

    public ReservationController(ReservationProducerService producerService,
                                  ReservationConfirmationService confirmationService) {
        this.producerService = producerService;
        this.confirmationService = confirmationService;
    }

    @PostMapping
    public ResponseEntity<String> requestReservation(@Valid @RequestBody ReservationRequestDto request) {
        producerService.requestSeatHold(request.showId(), request.seatId(), request.userId());
        return ResponseEntity.accepted().body("Solicitud de reserva recibida, procesando...");
    }

    @PutMapping("/confirm")
    public ResponseEntity<String> confirmReservation(@Valid @RequestBody ReservationRequestDto request) {
        ConfirmationResult result = confirmationService.confirm(request.showId(), request.seatId(), request.userId());
        return ResponseEntity.status(result.httpStatus()).body(result.message());
    }
}
