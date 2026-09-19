package com.portfolio.reservas;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;
@SpringBootApplication
@EnableScheduling
public class ReservasAppApplication {
        public static void main(String[] args) {
                SpringApplication.run(ReservasAppApplication.class, args);
        }
}
