package com.portfolio.reservas.repository;
import com.portfolio.reservas.domain.Reservation;
import com.portfolio.reservas.domain.enums.ReservationStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;
import java.util.UUID;
public interface ReservationRepository extends JpaRepository<Reservation, UUID> {
    List<Reservation> findBySeatIdAndStatus(UUID seatId, ReservationStatus status);
    List<Reservation> findByShowIdAndUserIdAndStatus(UUID showId, String userId, ReservationStatus status);
}
