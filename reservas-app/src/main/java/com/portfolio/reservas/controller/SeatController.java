package com.portfolio.reservas.controller;

import com.portfolio.reservas.controller.dto.SeatResponseDto;
import com.portfolio.reservas.repository.SeatRepository;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/shows")
public class SeatController {

    private final SeatRepository seatRepository;

    public SeatController(SeatRepository seatRepository) {
        this.seatRepository = seatRepository;
    }

    @GetMapping("/{showId}/seats")
    public List<SeatResponseDto> getSeatsByShow(@PathVariable UUID showId) {
        return seatRepository.findByShowId(showId).stream()
                .map(SeatResponseDto::from)
                .toList();
    }
}
