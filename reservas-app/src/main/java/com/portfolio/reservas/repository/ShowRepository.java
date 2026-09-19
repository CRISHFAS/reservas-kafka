package com.portfolio.reservas.repository;

import com.portfolio.reservas.domain.Show;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.UUID;

public interface ShowRepository extends JpaRepository<Show, UUID> {
}
