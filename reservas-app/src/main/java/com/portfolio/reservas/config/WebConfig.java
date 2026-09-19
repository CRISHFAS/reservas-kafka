package com.portfolio.reservas.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * Habilita CORS para los endpoints REST (/api/**). El frontend corre en un
 * origen distinto (puerto 3000) al backend (puerto 8080) durante desarrollo,
 * y sin esto el navegador bloquea las llamadas fetch/axios aunque curl
 * funcione perfecto (CORS es una restricción exclusiva del navegador, no
 * del servidor ni de herramientas como curl).
 *
 * Se usa allowedOriginPatterns("*") en vez de una lista fija de orígenes
 * porque la IP pública de la instancia puede variar (ver incidentes de
 * Security Group en las Fases 6 y 7) y porque simplifica el desarrollo.
 * Mismo criterio ya aplicado al WebSocket en WebSocketConfig (Fase 5).
 */
@Configuration
public class WebConfig implements WebMvcConfigurer {

    @Override
    public void addCorsMappings(CorsRegistry registry) {
        registry.addMapping("/api/**")
                .allowedOriginPatterns("*")
                .allowedMethods("GET", "POST", "PUT", "DELETE", "OPTIONS")
                .allowedHeaders("*");
    }
}
