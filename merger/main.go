package main

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/sirupsen/logrus"
)

func panicErr(l *logrus.Entry, err error) {
	if err != nil {
		l.Panic(err)
	}
}

// OpenDatabase returns connection pool or panics
func OpenDatabase(l *logrus.Entry, ip, port, user, password, dbname string) *pgxpool.Pool {
	dsn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", ip, port, user, password, dbname)
	db, err := pgxpool.Connect(context.Background(), dsn)
	panicErr(l, err)

	conn, err := db.Acquire(context.Background())
	defer conn.Release()
	conn.Conn().Ping(context.Background())
	panicErr(l, err)

	return db
}

func copyTable1(l *logrus.Entry, srcDB, destDB *pgxpool.Pool, tablename, query, insertQuery string) {
	offset := 0
	limit := 1000 // Fetch 1000 records at a time

	for {
		paginatedQuery := fmt.Sprintf("%s OFFSET %d LIMIT %d", query, offset, limit)
		rows, err := srcDB.Query(context.Background(), paginatedQuery)

		if err != nil {
			l.Error("Error querying source database: ", err)
			time.Sleep(time.Second * 5)
			continue
		}

		if !rows.Next() {
			break // No more records to process
		}

		tx, err := destDB.Begin(context.Background())
		panicErr(l, err)

		batch := &pgx.Batch{}
		for rows.Next() {
			var id int64
			var uuid string
			var createdAt time.Time
			err := rows.Scan(&id, &uuid, &createdAt)
			panicErr(l, err)

			// Check if record already exists
			var exists bool
			err = destDB.QueryRow(context.Background(), fmt.Sprintf(`SELECT EXISTS (SELECT 1 FROM %s WHERE uuid = $1)`, tablename), uuid).Scan(&exists)
			panicErr(l, err)

			if exists {
				//l.Info("Record already exists, skipping uuid: ", uuid)
				continue
			}

			batch.Queue(insertQuery, uuid, createdAt)
		}

		// Execute batch
		l.Infof("inserting batch offset %d of len %d", offset, batch.Len())
		br := tx.SendBatch(context.Background(), batch)
		err = br.Close()
		if err != nil {
			l.Error("Error in batch execution: ", err)
			tx.Rollback(context.Background())
			time.Sleep(time.Second * 5)
			continue
		}

		// Commit transaction
		err = tx.Commit(context.Background())
		panicErr(l, err)

		// Move the offset
		offset += limit
	}
}

func copyTable2(l *logrus.Entry, srcDB, destDB *pgxpool.Pool, tablename, query, insertQuery string) {
	offset := 0
	limit := 1000 // Fetch 1000 records at a time

	for {
		paginatedQuery := fmt.Sprintf("%s OFFSET %d LIMIT %d", query, offset, limit)
		rows, err := srcDB.Query(context.Background(), paginatedQuery)

		if err != nil {
			l.Error("Error querying source database: ", err)
			time.Sleep(time.Second * 5)
			continue
		}

		if !rows.Next() {
			break // No more records to process
		}

		tx, err := destDB.Begin(context.Background())
		panicErr(l, err)

		batch := &pgx.Batch{}
		for rows.Next() {
			var id int64
			var uuid string
			var accessToken string
			var fkUserID int64
			var countryISOCode string
			var createdAt time.Time
			err := rows.Scan(&id, &uuid, &accessToken, &fkUserID, &countryISOCode, &createdAt)
			panicErr(l, err)

			// Check if record already exists
			var exists bool
			err = destDB.QueryRow(context.Background(), fmt.Sprintf(`SELECT EXISTS (SELECT 1 FROM %s WHERE uuid = $1)`, tablename), uuid).Scan(&exists)
			panicErr(l, err)

			if exists {
				//l.Info("Record already exists, skipping uuid: ", uuid)
				continue
			}

			batch.Queue(insertQuery, uuid, accessToken, countryISOCode, createdAt)
		}

		// Execute batch
		l.Infof("inserting batch offset %d of len %d", offset, batch.Len())
		br := tx.SendBatch(context.Background(), batch)
		err = br.Close()
		if err != nil {
			l.Error("Error in batch execution: ", err)
			tx.Rollback(context.Background())
			time.Sleep(time.Second * 5)
			continue
		}

		// Commit transaction
		err = tx.Commit(context.Background())
		panicErr(l, err)

		// Move the offset
		offset += limit
	}
}

func copyTable3(l *logrus.Entry, srcDB, destDB *pgxpool.Pool, tablename, query, insertQuery string) {
	offset := 0
	limit := 50000 // Fetch 1000 records at a time

	for {
		paginatedQuery := fmt.Sprintf("%s OFFSET %d LIMIT %d", query, offset, limit)
		rows, err := srcDB.Query(context.Background(), paginatedQuery)

		if err != nil {
			l.Error("Error querying source database: ", err)
			time.Sleep(time.Second * 5)
			continue
		}

		if !rows.Next() {
			break // No more records to process
		}

		tx, err := destDB.Begin(context.Background())
		panicErr(l, err)

		batch := &pgx.Batch{}
		for rows.Next() {
			var fkSessionID int64
			var category string
			var key string
			var value string
			var createdAt time.Time
			err := rows.Scan(&fkSessionID, &category, &key, &value, &createdAt)
			panicErr(l, err)

			//// Check if record already exists
			//var exists bool
			//err = destDB.QueryRow(context.Background(), fmt.Sprintf(`SELECT EXISTS (SELECT 1 FROM %s WHERE uuid = $1)`, tablename), uuid).Scan(&exists)
			//panicErr(l, err)
			//
			//if exists {
			//	//l.Info("Record already exists, skipping uuid: ", uuid)
			//	continue
			//}

			batch.Queue(insertQuery, fkSessionID, category, key, value, createdAt)
		}

		// Execute batch
		l.Infof("inserting batch offset %d of len %d", offset, batch.Len())
		br := tx.SendBatch(context.Background(), batch)
		err = br.Close()
		if err != nil {
			l.Error("Error in batch execution: ", err)
			tx.Rollback(context.Background())
			time.Sleep(time.Second * 5)
			continue
		}

		// Commit transaction
		err = tx.Commit(context.Background())
		panicErr(l, err)

		// Move the offset
		offset += limit
	}
}

func main() {
	logger := logrus.New()
	l := logger.WithFields(logrus.Fields{"context": "main"})

	// Connect to source and destination databases
	srcDB := OpenDatabase(l, "127.0.0.1", "5433", "postgres", "postgres", "postgres")
	destDB := OpenDatabase(l, "127.0.0.1", "5555", "postgres", "***REMOVED***", "fpanalytics")

	// You can add copy commands for other tables in a similar way
	copyTable1(l, srcDB, destDB, `"user"`, `SELECT * FROM "user"`, `INSERT INTO "user" (uuid, created_at) VALUES ($1, $2)`)
	//copyTable2(l, srcDB, destDB, `"session"`, `SELECT * FROM "session"`, `INSERT INTO "session" (uuid, access_token, fk_user_id, country_iso_code, created_at) VALUES ($1, $2, CAST(1 AS BIGINT), $3, $4)`)
	//copyTable3(l, srcDB, destDB, `"event"`, `SELECT * FROM "event"`, `INSERT INTO "event" (fk_session_id, "category", "key", "value", created_at) VALUES ($1, $2, $3, $4, $5)`)

	// Close connections
	srcDB.Close()
	destDB.Close()
}
