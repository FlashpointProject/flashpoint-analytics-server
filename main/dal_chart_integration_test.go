package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

const chartTestUserUUID = "22222222-2222-4222-8222-222222222222"

func TestChartDALQueriesIntegration(t *testing.T) {
	if os.Getenv("ANALYTICS_INTEGRATION_TESTS") != "1" {
		t.Skip("set ANALYTICS_INTEGRATION_TESTS=1 to run database integration tests")
	}
	_ = godotenv.Load("../.env")

	ctx := context.Background()
	db := openTestDB(t)

	to := time.Date(2030, 5, 27, 12, 0, 0, 0, time.UTC)
	if err := seedChartTestData(ctx, db, to); err != nil {
		db.Close()
		t.Fatalf("seedChartTestData: %v", err)
	}
	t.Cleanup(func() {
		if err := cleanupChartTestData(context.Background(), db); err != nil {
			t.Fatalf("cleanupChartTestData: %v", err)
		}
		db.Close()
	})

	dal := &DAL{pool: db, sessionCache: NewSessionCache()}
	from := to.Add(-24 * time.Hour)

	activeNow, err := dal.GetActiveNow(ctx, to.Add(-15*time.Minute), to)
	if err != nil {
		t.Fatalf("GetActiveNow: %v", err)
	}
	if activeNow != 2 {
		t.Fatalf("activeNow = %d, want 2 distinct sessions despite duplicate events", activeNow)
	}

	activeUsers, err := dal.GetActiveUsersChart(ctx, from, to, time.Hour)
	if err != nil {
		t.Fatalf("GetActiveUsersChart: %v", err)
	}
	var foundActiveBucket bool
	for _, bucket := range activeUsers {
		if bucket.Time != nil && bucket.Time.Equal(to.Add(-2*time.Hour)) {
			foundActiveBucket = true
			if bucket.Count == nil || *bucket.Count != 2 {
				t.Fatalf("active bucket count = %v, want 2", bucket.Count)
			}
			if bucket.PerCountry["US"] != 1 || bucket.PerCountry["CZ"] != 1 {
				t.Fatalf("per-country = %#v, want US=1 CZ=1", bucket.PerCountry)
			}
		}
	}
	if !foundActiveBucket {
		t.Fatal("expected active user bucket not found")
	}

	countries, err := dal.GetActiveCountries(ctx, from, to)
	if err != nil {
		t.Fatalf("GetActiveCountries: %v", err)
	}
	countryCounts := map[string]uint64{}
	for _, country := range countries {
		countryCounts[country.CountryISO] = country.Count
	}
	if countryCounts["US"] != 1 || countryCounts["CZ"] != 1 {
		t.Fatalf("countryCounts = %#v, want US=1 CZ=1", countryCounts)
	}

	games, err := dal.GetTopGames(ctx, from, to, 2)
	if err != nil {
		t.Fatalf("GetTopGames: %v", err)
	}
	if len(games) != 2 {
		t.Fatalf("len(games) = %d, want 2", len(games))
	}
	if games[0].ID != "game-a" || games[0].PlayCount != 3 {
		t.Fatalf("top game = %#v, want game-a count 3", games[0])
	}
	if games[1].ID != "game-b" || games[1].PlayCount != 2 {
		t.Fatalf("second game = %#v, want game-b count 2", games[1])
	}

	usGames, err := dal.GetTopGamesByCountry(ctx, from, to, 2, "US")
	if err != nil {
		t.Fatalf("GetTopGamesByCountry US: %v", err)
	}
	if len(usGames) != 1 || usGames[0].ID != "game-a" || usGames[0].PlayCount != 2 {
		t.Fatalf("US games = %#v, want game-a count 2", usGames)
	}

	czGames, err := dal.GetTopGamesByCountry(ctx, from, to, 2, "CZ")
	if err != nil {
		t.Fatalf("GetTopGamesByCountry CZ: %v", err)
	}
	if len(czGames) != 2 {
		t.Fatalf("len(czGames) = %d, want 2", len(czGames))
	}
	if czGames[0].ID != "game-b" || czGames[0].PlayCount != 2 {
		t.Fatalf("CZ top game = %#v, want game-b count 2", czGames[0])
	}
	if czGames[1].ID != "game-a" || czGames[1].PlayCount != 1 {
		t.Fatalf("CZ second game = %#v, want game-a count 1", czGames[1])
	}

	unknownCountryGames, err := dal.GetTopGamesByCountry(ctx, from, to, 2, "ZZ")
	if err != nil {
		t.Fatalf("GetTopGamesByCountry ZZ: %v", err)
	}
	if len(unknownCountryGames) != 0 {
		t.Fatalf("unknown country games = %#v, want empty", unknownCountryGames)
	}

	hardware, err := dal.GetHardwareTrends(ctx, from, to, time.Hour, nil, nil)
	if err != nil {
		t.Fatalf("GetHardwareTrends: %v", err)
	}
	if !hasHardwareRow(hardware, to.Add(-3*time.Hour), "memory", "16 GB", 2) {
		t.Fatalf("hardware rows missing memory trend: %#v", hardware)
	}
	if !hasHardwareRow(hardware, to.Add(-3*time.Hour), "operatingSystem", "Windows 11 Home", 1) {
		t.Fatalf("hardware rows missing OS trend: %#v", hardware)
	}

	us := "US"
	usHardware, err := dal.GetHardwareTrends(ctx, from, to, time.Hour, nil, &us)
	if err != nil {
		t.Fatalf("GetHardwareTrends US: %v", err)
	}
	if !hasHardwareRow(usHardware, to.Add(-3*time.Hour), "memory", "16 GB", 1) {
		t.Fatalf("US hardware rows missing memory trend: %#v", usHardware)
	}
	if !hasHardwareRow(usHardware, to.Add(-3*time.Hour), "arch", "x64", 1) {
		t.Fatalf("US hardware rows missing arch trend: %#v", usHardware)
	}
	if hasHardwareRow(usHardware, to.Add(-3*time.Hour), "memory", "16 GB", 2) {
		t.Fatalf("US hardware rows include other countries: %#v", usHardware)
	}

	cz := "CZ"
	czHardware, err := dal.GetHardwareTrends(ctx, from, to, time.Hour, nil, &cz)
	if err != nil {
		t.Fatalf("GetHardwareTrends CZ: %v", err)
	}
	if !hasHardwareRow(czHardware, to.Add(-3*time.Hour), "memory", "16 GB", 1) {
		t.Fatalf("CZ hardware rows missing memory trend: %#v", czHardware)
	}
	if hasHardwareRow(czHardware, to.Add(-3*time.Hour), "arch", "x64", 1) {
		t.Fatalf("CZ hardware rows include US arch trend: %#v", czHardware)
	}

	memoryKey := "memory"
	usMemoryHardware, err := dal.GetHardwareTrends(ctx, from, to, time.Hour, &memoryKey, &us)
	if err != nil {
		t.Fatalf("GetHardwareTrends US memory: %v", err)
	}
	if !hasHardwareRow(usMemoryHardware, to.Add(-3*time.Hour), "memory", "16 GB", 1) {
		t.Fatalf("US memory hardware rows missing memory trend: %#v", usMemoryHardware)
	}
	if containsHardwareKey(usMemoryHardware, "arch") || containsHardwareKey(usMemoryHardware, "operatingSystem") {
		t.Fatalf("US memory hardware rows include other hardware keys: %#v", usMemoryHardware)
	}

	archKey := "arch"
	czArchHardware, err := dal.GetHardwareTrends(ctx, from, to, time.Hour, &archKey, &cz)
	if err != nil {
		t.Fatalf("GetHardwareTrends CZ arch: %v", err)
	}
	if len(czArchHardware) != 0 {
		t.Fatalf("CZ arch hardware rows = %#v, want empty", czArchHardware)
	}

	heatmapTarget := to.Add(-2 * time.Hour)
	heatmap, err := dal.GetActivityHeatmap(ctx, heatmapTarget.Add(-8*24*time.Hour), to, nil)
	if err != nil {
		t.Fatalf("GetActivityHeatmap: %v", err)
	}
	if !hasActivityHeatmapRow(heatmap, isoWeekday(heatmapTarget), heatmapTarget.Hour(), 3) {
		t.Fatalf("heatmap rows missing deduped UTC session-hour aggregate: %#v", heatmap)
	}

	usHeatmap, err := dal.GetActivityHeatmap(ctx, heatmapTarget.Add(-8*24*time.Hour), to, &us)
	if err != nil {
		t.Fatalf("GetActivityHeatmap US: %v", err)
	}
	if !hasActivityHeatmapRow(usHeatmap, isoWeekday(heatmapTarget), heatmapTarget.Hour(), 2) {
		t.Fatalf("US heatmap rows missing country-filtered deduped aggregate: %#v", usHeatmap)
	}

	czHeatmap, err := dal.GetActivityHeatmap(ctx, heatmapTarget.Add(-8*24*time.Hour), to, &cz)
	if err != nil {
		t.Fatalf("GetActivityHeatmap CZ: %v", err)
	}
	if !hasActivityHeatmapRow(czHeatmap, isoWeekday(heatmapTarget), heatmapTarget.Hour(), 1) {
		t.Fatalf("CZ heatmap rows missing country-filtered aggregate: %#v", czHeatmap)
	}
}

func openTestDB(t *testing.T) *pgxpool.Pool {
	t.Helper()

	port, err := strconv.ParseInt(os.Getenv("DB_PORT"), 10, 64)
	if err != nil {
		t.Fatalf("DB_PORT: %v", err)
	}
	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		os.Getenv("DB_IP"),
		port,
		os.Getenv("DB_USER"),
		os.Getenv("DB_PASSWORD"),
		os.Getenv("DB_NAME"),
	)

	db, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	return db
}

func seedChartTestData(ctx context.Context, db *pgxpool.Pool, now time.Time) error {
	if err := cleanupChartTestData(ctx, db); err != nil {
		return err
	}

	tx, err := db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var userID int64
	if err := tx.QueryRow(ctx, `INSERT INTO "user" ("uuid", "created_at") VALUES ($1, $2) RETURNING "id"`, chartTestUserUUID, now.Add(-14*24*time.Hour)).Scan(&userID); err != nil {
		return err
	}

	sessionDefs := []struct {
		uuid    string
		token   string
		country string
	}{
		{"22222222-2222-4222-8222-000000000001", "charttesttoken0000000000000001", "US"},
		{"22222222-2222-4222-8222-000000000002", "charttesttoken0000000000000002", "CZ"},
	}
	sessionIDs := make([]int64, 0, len(sessionDefs))
	for _, def := range sessionDefs {
		var sessionID int64
		if err := tx.QueryRow(ctx, `INSERT INTO "session" ("uuid", "access_token", "fk_user_id", "country_iso_code", "created_at") VALUES ($1, $2, $3, $4, $5) RETURNING "id"`, def.uuid, def.token, userID, def.country, now.Add(-14*24*time.Hour)).Scan(&sessionID); err != nil {
			return err
		}
		sessionIDs = append(sessionIDs, sessionID)
	}

	rows := [][]interface{}{
		{sessionIDs[0], "Activity", "heartbeat", "online", now.Add(-2 * time.Hour)},
		{sessionIDs[1], "Activity", "heartbeat", "online", now.Add(-2 * time.Hour)},
		{sessionIDs[0], "Activity", "heartbeat", "online", now.Add(-2*time.Hour + 15*time.Minute)},
		{sessionIDs[0], "Activity", "heartbeat", "online", now.Add((-7*24 - 2) * time.Hour)},
		{sessionIDs[0], "Activity", "heartbeat", "online", now.Add(-14 * time.Minute)},
		{sessionIDs[0], "Activity", "heartbeat", "online", now.Add(-1 * time.Minute)},
		{sessionIDs[1], "Activity", "heartbeat", "online", now.Add(-1 * time.Minute)},
		{sessionIDs[0], "Games", "gameLaunch", "game-a", now.Add(-4 * time.Hour)},
		{sessionIDs[0], "Games", "gameLaunch", "game-a", now.Add(-4*time.Hour + time.Minute)},
		{sessionIDs[1], "Games", "gameLaunch", "game-a", now.Add(-4*time.Hour + 2*time.Minute)},
		{sessionIDs[1], "Games", "gameLaunch", "game-b", now.Add(-5 * time.Hour)},
		{sessionIDs[1], "Games", "gameLaunch", "game-b", now.Add(-5*time.Hour + time.Minute)},
		{sessionIDs[0], "Hardware", "memory", "16 GB", now.Add(-3 * time.Hour)},
		{sessionIDs[1], "Hardware", "memory", "16 GB", now.Add(-3*time.Hour + time.Minute)},
		{sessionIDs[0], "Hardware", "arch", "x64", now.Add(-3 * time.Hour)},
		{sessionIDs[0], "Hardware", "operatingSystem", "Windows 11 Home", now.Add(-3 * time.Hour)},
	}

	if _, err := tx.CopyFrom(ctx, pgx.Identifier{"event"}, []string{"fk_session_id", "category", "key", "value", "created_at"}, pgx.CopyFromRows(rows)); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func cleanupChartTestData(ctx context.Context, db *pgxpool.Pool) error {
	tx, err := db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `DELETE FROM "event" WHERE "fk_session_id" IN (
		SELECT "session"."id" FROM "session"
		JOIN "user" ON "user"."id" = "session"."fk_user_id"
		WHERE "user"."uuid" = $1
	)`, chartTestUserUUID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM "session" USING "user" WHERE "session"."fk_user_id" = "user"."id" AND "user"."uuid" = $1`, chartTestUserUUID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM "user" WHERE "uuid" = $1`, chartTestUserUUID); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func hasHardwareRow(rows []HardwareTrendRow, bucket time.Time, key string, value string, count uint64) bool {
	for _, row := range rows {
		if row.Time.Equal(bucket) && row.Key == key && row.Value == value && row.Count == count {
			return true
		}
	}
	return false
}

func containsHardwareKey(rows []HardwareTrendRow, key string) bool {
	for _, row := range rows {
		if row.Key == key {
			return true
		}
	}
	return false
}

func hasActivityHeatmapRow(rows []ActivityHeatmapRow, dayOfWeek int, hour int, count uint64) bool {
	for _, row := range rows {
		if row.DayOfWeek == dayOfWeek && row.Hour == hour && row.ActiveSessions == count {
			return true
		}
	}
	return false
}

func isoWeekday(t time.Time) int {
	weekday := int(t.Weekday())
	if weekday == 0 {
		return 7
	}
	return weekday
}
