package main

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/Dri0m/flashpoint-analytics-server/utils"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/sirupsen/logrus"
)

// DBLayer is DBLayer
type DAL struct {
	pool         *pgxpool.Pool
	sessionCache *SessionCache
}

type SessionCache struct {
	m map[string]*Session
	sync.RWMutex
}

type Session struct {
	ID    int64
	Token string
}

func NewSessionCache() *SessionCache {
	return &SessionCache{
		m: make(map[string]*Session),
	}
}

func (s *SessionCache) Put(k string, v *Session) {
	s.Lock()
	defer s.Unlock()
	s.m[k] = v
}

func (s *SessionCache) Get(k string) *Session {
	s.RLock()
	defer s.RUnlock()
	v, ok := s.m[k]
	if !ok {
		return nil
	}
	return v
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

// GetSessionCached returns session, from cache or database
func (db *DAL) GetSessionCached(ctx context.Context, sessionUUID string) (*Session, error) {
	s := db.sessionCache.Get(sessionUUID)
	if s != nil {
		return s, nil
	}

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Release()

	s = &Session{}

	err = conn.QueryRow(ctx, `SELECT "id", "access_token" FROM "session" WHERE "uuid"=$1`, sessionUUID).Scan(&s.ID, &s.Token)
	if err != nil {
		return nil, err
	}

	return s, nil
}

// CreateSession returns a session UUID and a token, also creates the user if needed.
func (db *DAL) CreateSession(ctx context.Context, userUUID string, countryISO string) (*string, *string, error) {
	// INSERT INTO "user" ("uuid", "created_at") VALUES('68f96ad4-2690-479b-bb06-09e897d90e79', NOW()) ON CONFLICT DO NOTHING
	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer conn.Release()

	tx, err := conn.Begin(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer tx.Rollback(ctx)

	_, err = conn.Exec(ctx, `INSERT INTO "user"("uuid", "created_at") VALUES ($1, NOW()) ON CONFLICT DO NOTHING`, userUUID)
	if err != nil {
		return nil, nil, err
	}

	var userID int64
	err = conn.QueryRow(ctx, `SELECT "id" FROM "user" WHERE "uuid"=$1`, userUUID).Scan(&userID)
	if err != nil {
		return nil, nil, err
	}

	// TODO GEOIP

	sessionUUID := uuid.New().String()
	sessionToken := utils.RandomString(32)
	var sessionID int64
	err = conn.QueryRow(ctx, `INSERT INTO "session"("uuid", "access_token", "fk_user_id", "country_iso_code", "created_at") VALUES ($1, $2, $3, $4, NOW()) RETURNING id`, sessionUUID, sessionToken, userID, countryISO).Scan(&sessionID)
	if err != nil {
		return nil, nil, err
	}

	err = tx.Commit(ctx)
	if err != nil {
		return nil, nil, err
	}

	db.sessionCache.Put(sessionUUID, &Session{ID: sessionID, Token: sessionToken})
	return &sessionUUID, &sessionToken, nil
}

func (db *DAL) InsertEntryBatch(ctx context.Context, rows [][]interface{}) error {
	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

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

	return nil
}

// test stuff

func (db *DAL) InsertFooData(ctx context.Context) error {
	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	for i := 0; i < 1000; i++ {

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
		err = conn.QueryRow(ctx, `INSERT INTO "session"("uuid", "access_token", "fk_user_id", "country_iso_code", "created_at") VALUES ($1, $2, $3, $4, NOW()) RETURNING id`, uuid.New().String(), utils.RandomString(32), userID, "test").Scan(&sessionID)
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
	min := time.Date(2022, 1, 0, 0, 0, 0, 0, time.UTC).Unix()
	max := time.Now().Unix()
	delta := max - min

	sec := rand.Int63n(delta) + min
	return time.Unix(sec, 0)
}
