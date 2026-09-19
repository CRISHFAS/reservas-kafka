package com.portfolio.reservas.service;

public record ConfirmationResult(boolean success, int httpStatus, String message) {
    public static ConfirmationResult ok(String message) {
        return new ConfirmationResult(true, 200, message);
    }
    public static ConfirmationResult conflict(String message) {
        return new ConfirmationResult(false, 409, message);
    }
    public static ConfirmationResult notFound(String message) {
        return new ConfirmationResult(false, 404, message);
    }
}
