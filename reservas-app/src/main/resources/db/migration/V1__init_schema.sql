CREATE TABLE shows (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    title VARCHAR(200) NOT NULL,
    room VARCHAR(50) NOT NULL,
    start_time TIMESTAMP NOT NULL,
    total_seats INT NOT NULL
);

CREATE TABLE seats (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    show_id UUID NOT NULL REFERENCES shows(id),
    seat_row VARCHAR(5) NOT NULL,
    seat_number INT NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'AVAILABLE',
    held_by VARCHAR(100),
    held_until TIMESTAMP,
    version BIGINT NOT NULL DEFAULT 0,
    UNIQUE(show_id, seat_row, seat_number)
);

CREATE TABLE reservations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    show_id UUID NOT NULL REFERENCES shows(id),
    seat_id UUID NOT NULL REFERENCES seats(id),
    user_id VARCHAR(100) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    expires_at TIMESTAMP
);

CREATE INDEX idx_seats_show_id ON seats(show_id);
CREATE INDEX idx_reservations_show_id ON reservations(show_id);
