package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

const seedUserUUID = "11111111-1111-4111-8111-111111111111"
const seedDays = 400

type config struct {
	DBUser     string
	DBPassword string
	DBIP       string
	DBPort     int64
	DBName     string
}

func envString(name string) string {
	s := os.Getenv(name)
	if s == "" {
		panic(fmt.Sprintf("env variable %q is not set", name))
	}
	return s
}

func envInt(name string) int64 {
	i, err := strconv.ParseInt(envString(name), 10, 64)
	if err != nil {
		panic(err)
	}
	return i
}

func getConfig() *config {
	return &config{
		DBUser:     envString("DB_USER"),
		DBPassword: envString("DB_PASSWORD"),
		DBIP:       envString("DB_IP"),
		DBPort:     envInt("DB_PORT"),
		DBName:     envString("DB_NAME"),
	}
}

func main() {
	if err := godotenv.Load(); err != nil {
		panic(err)
	}

	ctx := context.Background()
	conf := getConfig()
	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", conf.DBIP, conf.DBPort, conf.DBUser, conf.DBPassword, conf.DBName)
	db, err := pgxpool.New(ctx, dsn)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	if err := seed(ctx, db); err != nil {
		panic(err)
	}
	fmt.Println("seeded analytics demo data")
}

func seed(ctx context.Context, db *pgxpool.Pool) error {
	tx, err := db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `DELETE FROM "event" WHERE "fk_session_id" IN (
		SELECT "session"."id" FROM "session"
		JOIN "user" ON "user"."id" = "session"."fk_user_id"
		WHERE "user"."uuid" = $1
	)`, seedUserUUID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM "session" USING "user" WHERE "session"."fk_user_id" = "user"."id" AND "user"."uuid" = $1`, seedUserUUID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM "user" WHERE "uuid" = $1`, seedUserUUID); err != nil {
		return err
	}

	var userID int64
	now := time.Now().UTC().Truncate(time.Minute)
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	seedStart := today.AddDate(0, 0, -seedDays)

	if err := tx.QueryRow(ctx, `INSERT INTO "user" ("uuid", "created_at") VALUES ($1, $2) RETURNING "id"`, seedUserUUID, seedStart).Scan(&userID); err != nil {
		return err
	}

	sessions := []struct {
		country         string
		memory          string
		arch            string
		operatingSystem string
	}{
		{"US", "16 GB", "x64", "Windows 11 Home"},
		{"CZ", "16 GB", "x64", "Windows 10 Pro"},
		{"DE", "32 GB", "x64", "Windows 11 Pro"},
		{"GB", "8 GB", "x64", "Windows 10 Home"},
		{"BR", "16 GB", "x64", "Linux"},
		{"CA", "32 GB", "arm64", "macOS 14"},
		{"AU", "16 GB", "x64", "Windows 11 Home"},
		{"JP", "8 GB", "x64", "Windows 10 Pro"},
		{"FR", "64 GB", "x64", "Windows 11 Pro"},
		{"PL", "16 GB", "x64", "Linux"},
		{"NL", "32 GB", "arm64", "macOS 15"},
		{"SE", "8 GB", "x64", "Windows 10 Home"},
	}

	sessionIDs := make([]int64, 0, len(sessions))
	for idx, session := range sessions {
		var sessionID int64
		sessionUUID := fmt.Sprintf("11111111-1111-4111-8111-%012d", idx+1)
		sessionToken := fmt.Sprintf("seedtoken%023d", idx+1)
		createdAt := seedStart.Add(time.Duration(idx) * time.Hour)
		if err := tx.QueryRow(ctx, `INSERT INTO "session" ("uuid", "access_token", "fk_user_id", "country_iso_code", "created_at") VALUES ($1, $2, $3, $4, $5) RETURNING "id"`, sessionUUID, sessionToken, userID, session.country, createdAt).Scan(&sessionID); err != nil {
			return err
		}
		sessionIDs = append(sessionIDs, sessionID)
	}

	rows := make([][]interface{}, 0, 30000)
	activityHours := []int{0, 3, 7, 12, 16, 20, 23}
	for day := seedDays; day >= 0; day-- {
		dayStart := today.AddDate(0, 0, -day)
		for _, hour := range activityHours {
			t := dayStart.Add(time.Duration(hour)*time.Hour + time.Duration((day+hour)%45)*time.Minute)
			if t.After(now) {
				continue
			}
			for idx, sessionID := range sessionIDs {
				if (day+hour+idx)%4 == 0 {
					continue
				}
				rows = append(rows, []interface{}{sessionID, "Activity", "heartbeat", "online", t})
			}
		}
	}
	for _, sessionID := range sessionIDs {
		rows = append(rows, []interface{}{sessionID, "Activity", "heartbeat", "online", now.Add(-5 * time.Minute)})
	}

	games := []struct {
		id     string
		weight int
	}{
		{"fbaf2901-b188-4415-978b-c108dc8cb394", 5}, // Snow Lodge Escape
		{"51240ae7-1fa8-4ee7-8d64-9dbf5019eab8", 5}, // Poptropica (AS2)
		{"215c9f84-905e-4d3e-bac8-4b381d27f0e7", 4}, // Line Rider
		{"1227d57e-1ff7-48d6-bbb3-82649b192ee1", 4}, // The Fancy Pants Adventures: World 4 part 1
		{"16c27895-f7a6-6c65-18f3-feebcf87d28a", 3}, // Papa Louie 2: When Burgers Attack!
		{"001058a2-9303-4857-8160-d9ead3c53829", 3}, // Running Fred
		{"140c4ef1-4661-400b-8973-9dd7362a1158", 2}, // Achievement Unlocked
		{"08143aa7-f3ae-45b0-a1d4-afa4ac44c845", 2}, // Alien Hominid
		{"6a0b0bd9-13cd-4988-bacf-2f4d9ebd0753", 1}, // Bloons Tower Defense
		{"e7d29d57-5ecf-4e0f-8f40-e6b62a80674a", 1}, // Motherload
	}
	for day := seedDays; day >= 0; day-- {
		dayStart := today.AddDate(0, 0, -day)
		for gameIdx, game := range games {
			if (day+gameIdx)%6 >= game.weight {
				continue
			}
			sessionID := sessionIDs[(day+gameIdx)%len(sessionIDs)]
			t := dayStart.Add(time.Duration((gameIdx*2+day)%24)*time.Hour + time.Duration(gameIdx*5)*time.Minute)
			if t.After(now) {
				continue
			}
			rows = append(rows, []interface{}{sessionID, "Games", "gameLaunch", game.id, t})
		}
	}

	for day := seedDays; day >= 0; day -= 7 {
		dayStart := today.AddDate(0, 0, -day)
		for idx, session := range sessions {
			t := dayStart.Add(time.Duration(idx%12) * time.Hour)
			if t.After(now) {
				continue
			}
			sessionID := sessionIDs[idx]
			rows = append(rows, []interface{}{sessionID, "Hardware", "memory", session.memory, t})
			rows = append(rows, []interface{}{sessionID, "Hardware", "arch", session.arch, t.Add(time.Minute)})
			rows = append(rows, []interface{}{sessionID, "Hardware", "operatingSystem", session.operatingSystem, t.Add(2 * time.Minute)})
		}
	}

	if _, err := tx.CopyFrom(
		ctx,
		pgx.Identifier{"event"},
		[]string{"fk_session_id", "category", "key", "value", "created_at"},
		pgx.CopyFromRows(rows),
	); err != nil {
		return err
	}

	return tx.Commit(ctx)
}
