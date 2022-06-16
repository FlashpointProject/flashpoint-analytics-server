CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

CREATE TABLE IF NOT EXISTS "user" (
  "id" BIGINT GENERATED ALWAYS AS IDENTITY,

  "uuid" CHAR(36) NOT NULL UNIQUE,
  "created_at" TIMESTAMPTZ NOT NULL,

  PRIMARY KEY("id")
);

CREATE TABLE IF NOT EXISTS "session" (
  "id" BIGINT GENERATED ALWAYS AS IDENTITY,

  "uuid" CHAR(36) NOT NULL UNIQUE,
  "access_token" CHAR(32) NOT NULL,
  "fk_user_id" BIGINT NOT NULL,
  "country_geoname_id" INT, -- extracted from IP address
  "created_at" TIMESTAMPTZ NOT NULL,

  PRIMARY KEY("id"),
  CONSTRAINT "c_fk_user_id" FOREIGN KEY("fk_user_id") REFERENCES "user"("id")
);

CREATE TABLE IF NOT EXISTS "entry" (
  "fk_session_id" BIGINT NOT NULL,
  "category" VARCHAR(126) NOT NULL,
  "key" VARCHAR(126) NOT NULL,
  "value" TEXT NOT NULL,
  "created_at" TIMESTAMPTZ NOT NULL,
  
  CONSTRAINT "c_fk_session_id" FOREIGN KEY("fk_session_id") REFERENCES "session"("id")
);

SELECT create_hypertable('entry', 'created_at');