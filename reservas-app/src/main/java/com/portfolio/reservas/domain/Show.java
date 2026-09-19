package com.portfolio.reservas.domain;

import jakarta.persistence.*;
import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(name = "shows")
public class Show {

    @Id
    @GeneratedValue
    private UUID id;

    @Column(nullable = false, length = 200)
    private String title;

    @Column(nullable = false, length = 50)
    private String room;

    @Column(name = "start_time", nullable = false)
    private LocalDateTime startTime;

    @Column(name = "total_seats", nullable = false)
    private Integer totalSeats;

    protected Show() {
    }

    public Show(String title, String room, LocalDateTime startTime, Integer totalSeats) {
        this.title = title;
        this.room = room;
        this.startTime = startTime;
        this.totalSeats = totalSeats;
    }

    public UUID getId() {
        return id;
    }

    public String getTitle() {
        return title;
    }

    public String getRoom() {
        return room;
    }

    public LocalDateTime getStartTime() {
        return startTime;
    }

    public Integer getTotalSeats() {
        return totalSeats;
    }
}
