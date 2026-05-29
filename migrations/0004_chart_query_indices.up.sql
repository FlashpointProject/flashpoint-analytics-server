CREATE INDEX IF NOT EXISTS event_category_key_created_at_session_idx
ON event (category, key, created_at, fk_session_id);
