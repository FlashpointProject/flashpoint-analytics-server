package main

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Dri0m/flashpoint-analytics-server/utils"
	"github.com/gorilla/mux"
	"github.com/joho/godotenv"
	"github.com/sirupsen/logrus"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

func panicErr(l *logrus.Entry, err error) {
	if err != nil {
		l.Fatal(err)
	}
}

// OpenDatabase returns connection pool or panics
func OpenDatabase(l *logrus.Entry, conf *Config) *pgxpool.Pool {
	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", conf.DBIP, conf.DBPort, conf.DBUser, conf.DBPassword, conf.DBName) // TODO move to config
	db, err := pgxpool.Connect(context.Background(), dsn)
	panicErr(l, err)

	conn, err := db.Acquire(context.Background())
	defer conn.Release()
	conn.Conn().Ping(context.Background())
	panicErr(l, err)

	return db
}

func main() {
	err := godotenv.Load()
	if err != nil {
		panic(err)
	}

	log := utils.InitLogger()
	conf := GetConfig(log)
	l := log.WithField("commit", EnvString("GIT_COMMIT"))

	l.Infoln("hi")

	db := OpenDatabase(l, conf)
	defer db.Close()

	app := App{
		DAL: &DAL{pool: db},
		l:   l,
	}

	r := mux.NewRouter()
	r.HandleFunc("/collect", CollectAuth(conf.AccessKey, app.CollectHandler)).Methods("POST")
	r.HandleFunc("/fill-fake-data", app.FillFakeDataHandler).Methods("GET")
	http.Handle("/", r)

	srv := &http.Server{
		Handler: utils.LogRequestHandler(l, r),
		Addr:    fmt.Sprintf("127.0.0.1:%d", conf.Port),
		// Good practice: enforce timeouts for servers you create!
		WriteTimeout: 60 * time.Second,
		ReadTimeout:  60 * time.Second,
	}

	l.WithField("port", conf.Port).Infoln("starting the server...")
	go func() {
		srv.ListenAndServe()
	}()

	term := make(chan os.Signal, 1)
	signal.Notify(term, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	<-term

	l.Infoln("shutting down the server...")
	if err := srv.Shutdown(context.Background()); err != nil {
		l.WithError(err).Errorln("server shutdown failed")
	}

	l.Infoln("closing all db connections...")
	db.Close()

	l.Infoln("goodbye")
}

type App struct {
	DAL *DAL
	l   *logrus.Entry
}

// DBLayer is DBLayer
type DAL struct {
	pool *pgxpool.Pool
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

		// 		batch.Queue(`INSERT INTO "entry"("fk_session_id", "category", "key", "value", "created_at") VALUES ($1, $2, $3, $4, NOW())`, sessionID, "fake", "fake", "fake")

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
