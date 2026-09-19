package com.portfolio.reservas.repository;
import com.portfolio.reservas.domain.Seat;
import com.portfolio.reservas.domain.enums.SeatStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
public interface SeatRepository extends JpaRepository<Seat, UUID> {
    List<Seat> findByShowId(UUID showId);
    List<Seat> findByStatusAndHeldUntilBefore(SeatStatus status, LocalDateTime dateTime);
}
