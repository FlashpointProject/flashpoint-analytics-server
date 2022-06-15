package main

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/sirupsen/logrus"
)

// DBLayer is DBLayer
type DAL struct {
	pool *pgxpool.Pool
}

// OpenDatabase returns connection pool or panics
func OpenDatabase(l *logrus.Entry, conf *Config) *pgxpool.Pool {
	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", conf.DBIP, conf.DBPort, conf.DBUser, conf.DBPassword, conf.DBName)
	db, err := pgxpool.Connect(context.Background(), dsn)
	panicErr(l, err)

	conn, err := db.Acquire(context.Background())
	defer conn.Release()
	conn.Conn().Ping(context.Background())
	panicErr(l, err)

	return db
}

func (db *DAL) InsertFooData(ctx context.Context) error {
	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	for i := 0; i < 10000; i++ {

		tx, err := conn.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)

		userID := 0
		err = conn.QueryRow(ctx, `INSERT INTO "user"("uuid", "created_at") VALUES ($1, NOW()) RETURNING id`, uuid.New().String()).Scan(&userID)
		if err != nil {
			return err
		}

		sessionID := 0
		err = conn.QueryRow(ctx, `INSERT INTO "session"("uuid", "fk_user_id", "country_geoname_id", "created_at") VALUES ($1, $2, $3, NOW()) RETURNING id`, uuid.New().String(), userID, 42).Scan(&sessionID)
		if err != nil {
			return err
		}

		rows := [][]interface{}{}

		for i := 0; i < 1000; i++ {
			rows = append(rows, []interface{}{sessionID, "fake", "fake", "fake", randate()})
		}

		_, err = conn.CopyFrom(
			ctx,
			pgx.Identifier{"entry"},
			[]string{"fk_session_id", "category", "key", "value", "created_at"},
			pgx.CopyFromRows(rows),
		)
		if err != nil {
			return err
		}

		err = tx.Commit(ctx)
		if err != nil {
			return err
		}
	}

	return nil

}

func randate() time.Time {
	min := time.Date(2020, 1, 0, 0, 0, 0, 0, time.UTC).Unix()
	max := time.Date(2021, 1, 0, 0, 0, 0, 0, time.UTC).Unix()
	delta := max - min

	sec := rand.Int63n(delta) + min
	return time.Unix(sec, 0)
}
