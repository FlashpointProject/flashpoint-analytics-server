CREATE INDEX IF NOT EXISTS session_country_iso_code_id_idx
ON "session" (country_iso_code, id)
WHERE country_iso_code IS NOT NULL AND country_iso_code <> '';
