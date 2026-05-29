CREATE INDEX IF NOT EXISTS event_game_launch_created_at_session_idx
ON event (created_at, fk_session_id)
WHERE category = 'Games' AND key = 'gameLaunch';
