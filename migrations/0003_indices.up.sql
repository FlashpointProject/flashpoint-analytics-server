CREATE INDEX ON session (fk_user_id, created_at);
CREATE INDEX ON event (category, key, created_at);
