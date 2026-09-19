ALTER TABLE reservations ADD COLUMN event_id UUID;
ALTER TABLE reservations ADD CONSTRAINT uk_reservations_event_id UNIQUE (event_id);
