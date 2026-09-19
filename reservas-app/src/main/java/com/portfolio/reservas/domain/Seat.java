package com.portfolio.reservas.domain;

import com.portfolio.reservas.domain.enums.SeatStatus;
import jakarta.persistence.*;
import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(name = "seats", uniqueConstraints = @UniqueConstraint(columnNames = {"show_id", "seat_row", "seat_number"}))
public class Seat {

    @Id
    @GeneratedValue
    private UUID id;

    @Column(name = "show_id", nullable = false)
    private UUID showId;

    @Column(name = "seat_row", nullable = false, length = 5)
    private String row;

    @Column(name = "seat_number", nullable = false)
    private Integer number;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private SeatStatus status = SeatStatus.AVAILABLE;

    @Column(name = "held_by", length = 100)
    private String heldBy;

    @Column(name = "held_until")
    private LocalDateTime heldUntil;

    @Version
    private Long version;

    protected Seat() {
    }

    public Seat(UUID showId, String row, Integer number) {
        this.showId = showId;
        this.row = row;
        this.number = number;
    }

    public UUID getId() {
        return id;
    }

    public UUID getShowId() {
        return showId;
    }

    public String getRow() {
        return row;
    }

    public Integer getNumber() {
        return number;
    }

    public SeatStatus getStatus() {
        return status;
    }

    public void hold(String userId, LocalDateTime until) {
        this.status = SeatStatus.HELD;
        this.heldBy = userId;
        this.heldUntil = until;
    }

    public void confirm() {
        this.status = SeatStatus.CONFIRMED;
        this.heldUntil = null;
    }

    public void release() {
        this.status = SeatStatus.AVAILABLE;
        this.heldBy = null;
        this.heldUntil = null;
    }

    public String getHeldBy() {
        return heldBy;
    }

    public LocalDateTime getHeldUntil() {
        return heldUntil;
    }

    public Long getVersion() {
        return version;
    }
}
