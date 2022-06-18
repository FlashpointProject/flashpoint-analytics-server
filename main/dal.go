package main

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
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

func (db *DAL) InsertEventBatch(ctx context.Context, rows [][]interface{}) error {
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
		pgx.Identifier{"event"},
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

type CurrentlyOnline struct {
	Time       *time.Time       `json:"time"`
	Count      *int64           `json:"online_count"`
	PerCountry map[string]int64 `json:"per_country"`
}

// GetCurrentlyOnline returns sessions that produced events in a given timeframe
func (db *DAL) GetCurrentlyOnline(ctx context.Context, filter *CurrentlyOnlineRequest) ([]CurrentlyOnline, error) {
	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Release()

	rows, err := conn.Query(ctx, `
	SELECT "time",
	"online_count",
	count_elements((SELECT array_agg("session"."country_iso_code") FROM "session" WHERE "session"."id" = ANY("session_ids"))) as "per_country"
	FROM
	(SELECT 
	time_bucket_gapfill($1, event."created_at") AS "time", 
		count(DISTINCT "fk_session_id") AS "online_count",
		array_agg(DISTINCT event."fk_session_id") AS "session_ids"
	FROM event 
	JOIN "session" ON "session"."id"=event."fk_session_id"
	WHERE 
		event."created_at" <= to_timestamp($2) AND
		event."created_at" >= to_timestamp($3)
	GROUP BY "time"
	ORDER BY "time") AS t1
	`, fmt.Sprintf("%d", *filter.Interval), filter.To, filter.From)
	if err != nil {
		return nil, err
	}

	result := make([]CurrentlyOnline, 0)

	for rows.Next() {
		co := CurrentlyOnline{}
		co.PerCountry = make(map[string]int64)
		var rawPerCountry *string
		err := rows.Scan(&co.Time, &co.Count, &rawPerCountry)
		if err != nil {
			return nil, err
		}

		if rawPerCountry != nil {
			splitPerCountry := strings.Split(*rawPerCountry, ",")
			for _, tuple := range splitPerCountry {
				splitTuple := strings.Split(tuple, ":")
				if len(splitTuple) != 2 {
					return nil, fmt.Errorf("invalid data received from database")
				}

				countryISO := splitTuple[0]
				countryCount, err := strconv.ParseInt(splitTuple[1], 10, 64)
				if err != nil {
					return nil, fmt.Errorf("invalid data received from database")
				}
				co.PerCountry[countryISO] = countryCount
			}
		}

		result = append(result, co)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return result, nil
}

type RawEvent struct {
	SessionUUID       string    `json:"session_uuid"`
	SessionCountryISO string    `json:"session_country_iso"`
	SessionCreatedAt  time.Time `json:"session_created_at"`
	EventCategory     string    `json:"event_category"`
	EventKey          string    `json:"event_key"`
	EventValue        string    `json:"event_value"`
	EventCreatedAt    time.Time `json:"event_created_at"`
}

// GetRawEvents returns a list of filtered events
func (db *DAL) GetRawEvents(ctx context.Context, filter *RawEventsRequest) ([]RawEvent, error) {
	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Release()

	optionalFilterString := ""
	filterValues := make([]interface{}, 0)
	optionalFilterCounter := 3

	filterValues = append(filterValues, filter.To)
	filterValues = append(filterValues, filter.From)

	if filter.SessionFrom != nil {
		optionalFilterString += fmt.Sprintf(` AND "session"."created_at" >= to_timestamp($%d)`, optionalFilterCounter)
		filterValues = append(filterValues, *filter.SessionFrom)
		optionalFilterCounter++
	}
	if filter.SessionTo != nil {
		optionalFilterString += fmt.Sprintf(` AND "session"."created_at" <= to_timestamp(%d)`, optionalFilterCounter)
		filterValues = append(filterValues, *filter.SessionTo)
		optionalFilterCounter++
	}
	if filter.SessionCountryISO != nil {
		optionalFilterString += fmt.Sprintf(` AND "session"."country_iso_code" = $%d`, optionalFilterCounter)
		filterValues = append(filterValues, *filter.SessionCountryISO)
		optionalFilterCounter++
	}
	if filter.SessionUUID != nil {
		optionalFilterString += fmt.Sprintf(` AND "session"."uuid" = $%d`, optionalFilterCounter)
		filterValues = append(filterValues, *filter.SessionUUID)
		optionalFilterCounter++
	}
	if filter.EventCategory != nil {
		optionalFilterString += fmt.Sprintf(` AND "event"."category" = $%d`, optionalFilterCounter)
		filterValues = append(filterValues, *filter.EventCategory)
		optionalFilterCounter++
	}
	if filter.EventKey != nil {
		optionalFilterString += fmt.Sprintf(` AND "event"."key" = $%d`, optionalFilterCounter)
		filterValues = append(filterValues, *filter.EventKey)
		optionalFilterCounter++
	}
	if filter.EventValue != nil {
		optionalFilterString += fmt.Sprintf(` AND "event"."value" = $%d`, optionalFilterCounter)
		filterValues = append(filterValues, *filter.EventValue)
		optionalFilterCounter++
	}

	rows, err := conn.Query(ctx, `
	SELECT 
    "session"."uuid" as "session_uuid",
    "session"."country_iso_code" as "session_country_iso_code",
    "session"."created_at" as "session_created_at",
    "event"."category" as "event_category",
    "event"."key" as "event_key",
    "event"."value" as "event_value",
    "event"."created_at" as "event_created_at"
	FROM "event" 
	JOIN "session" ON "session"."id"="event"."fk_session_id"
	WHERE 
		"event"."created_at" <= to_timestamp($1) AND
		"event"."created_at" >= to_timestamp($2)
		`+optionalFilterString+`
	ORDER BY "event"."created_at"
	`, filterValues...)
	if err != nil {
		return nil, err
	}

	result := make([]RawEvent, 0)

	for rows.Next() {
		re := RawEvent{}
		err := rows.Scan(&re.SessionUUID, &re.SessionCountryISO, &re.SessionCreatedAt, &re.EventCategory, &re.EventKey, &re.EventValue, &re.EventCreatedAt)
		if err != nil {
			return nil, err
		}

		result = append(result, re)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return result, nil
}

type EventCount struct {
	EventCategory string `json:"event_category"`
	EventKey      string `json:"event_key"`
	EventValue    string `json:"event_value"`
	EventCount    uint64 `json:"event_count"`
}

// GetEventCount returns a list of filtered events, grouping by value
func (db *DAL) GetEventCount(ctx context.Context, filter *EventCountRequest) ([]EventCount, error) {
	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Release()

	optionalFilterString := ""
	filterValues := make([]interface{}, 0)
	optionalFilterCounter := 3

	filterValues = append(filterValues, filter.To)
	filterValues = append(filterValues, filter.From)

	if filter.EventCategory != nil {
		optionalFilterString += fmt.Sprintf(` AND "event"."category" = $%d`, optionalFilterCounter)
		filterValues = append(filterValues, *filter.EventCategory)
		optionalFilterCounter++
	}
	if filter.EventKey != nil {
		optionalFilterString += fmt.Sprintf(` AND "event"."key" = $%d`, optionalFilterCounter)
		filterValues = append(filterValues, *filter.EventKey)
		optionalFilterCounter++
	}
	if filter.EventValue != nil {
		optionalFilterString += fmt.Sprintf(` AND "event"."value" = $%d`, optionalFilterCounter)
		filterValues = append(filterValues, *filter.EventValue)
		optionalFilterCounter++
	}

	rows, err := conn.Query(ctx, `
	SELECT 
    "event"."category" as "event_category",
    "event"."key" as "event_key",
    "event"."value" as "event_value",
    COUNT("event"."value") as "event_value_count"
	FROM "event" 
	WHERE 
		"event"."created_at" <= to_timestamp($1) AND
		"event"."created_at" >= to_timestamp($2)
		`+optionalFilterString+`
	GROUP BY "event"."category", "event"."key", "event"."value"
	ORDER BY "event_value_count" DESC
	`, filterValues...)
	if err != nil {
		return nil, err
	}

	result := make([]EventCount, 0)

	for rows.Next() {
		ec := EventCount{}
		err := rows.Scan(&ec.EventCategory, &ec.EventKey, &ec.EventValue, &ec.EventCount)
		if err != nil {
			return nil, err
		}

		result = append(result, ec)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return result, nil
}

// test stuff

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
		err = conn.QueryRow(ctx, `INSERT INTO "session"("uuid", "access_token", "fk_user_id", "country_iso_code", "created_at") VALUES ($1, $2, $3, $4, NOW()) RETURNING id`, uuid.New().String(), utils.RandomString(32), userID, utils.RandomCountryISO()).Scan(&sessionID)
		if err != nil {
			return err
		}

		rows := [][]interface{}{}

		for i := 0; i < 1000; i++ {
			rows = append(rows, []interface{}{sessionID, utils.RandomString(1), utils.RandomString(1), utils.RandomString(1), randate()})
		}

		_, err = conn.CopyFrom(
			ctx,
			pgx.Identifier{"event"},
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
