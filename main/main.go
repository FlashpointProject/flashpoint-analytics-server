package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/Dri0m/flashpoint-analytics-server/utils"
	"github.com/gorilla/mux"
	"github.com/joho/godotenv"
	"github.com/oschwald/geoip2-golang"
	"github.com/sirupsen/logrus"
)

func panicErr(l *logrus.Entry, err error) {
	if err != nil {
		l.Fatal(err)
	}
}

type App struct {
	DAL          *DAL
	l            *logrus.Entry
	collectQueue chan *Event
	mmdb         *geoip2.Reader
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
		DAL: &DAL{
			pool:         db,
			sessionCache: NewSessionCache(),
		},
		l:            l,
		collectQueue: make(chan *Event, 100000),
	}

	mmdb, err := geoip2.Open("GeoLite2-Country.mmdb")
	if err != nil {
		log.Fatal(err)
	}
	defer mmdb.Close()
	app.mmdb = mmdb

	r := mux.NewRouter()

	r.HandleFunc("/user/{uuid}/session", app.KeyAuth(conf.AccessKey, app.InitSessionHandler)).Methods("GET")
	r.HandleFunc("/session/{uuid}/event", app.SessionAuth(app.CollectHandler)).Methods("POST")

	r.HandleFunc("/data/online", app.KeyAuth(conf.ReadKey, app.GetCurrentlyOnlineHandler)).Methods("POST")
	r.HandleFunc("/data/events-raw", app.KeyAuth(conf.ReadKey, app.GetRawEventsHandler)).Methods("POST")
	r.HandleFunc("/data/events-count", app.KeyAuth(conf.ReadKey, app.GetEventCountHandler)).Methods("POST")
	r.HandleFunc("/data/chart/active-now", app.KeyAuth(conf.ReadKey, app.GetActiveNowHandler)).Methods("POST")
	r.HandleFunc("/data/chart/active-users", app.KeyAuth(conf.ReadKey, app.GetActiveUsersChartHandler)).Methods("POST")
	r.HandleFunc("/data/chart/active-countries", app.KeyAuth(conf.ReadKey, app.GetActiveCountriesHandler)).Methods("POST")
	r.HandleFunc("/data/chart/top-games", app.KeyAuth(conf.ReadKey, app.GetTopGamesHandler)).Methods("POST")
	r.HandleFunc("/data/chart/game-launch-total", app.KeyAuth(conf.ReadKey, app.GetGameLaunchTotalHandler)).Methods("POST")
	r.HandleFunc("/data/chart/hardware-trends", app.KeyAuth(conf.ReadKey, app.GetHardwareTrendsHandler)).Methods("POST")
	r.HandleFunc("/data/chart/activity-heatmap", app.KeyAuth(conf.ReadKey, app.GetActivityHeatmapHandler)).Methods("GET", "POST")

	// r.HandleFunc("/fill-fake-data", app.FillFakeDataHandler).Methods("GET")
	// r.HandleFunc("/ingest-stuff", app.IngestStuffHandler).Methods("GET")
	http.Handle("/", r)

	ctx, cancelFunc := context.WithCancel(context.Background())

	wg := &sync.WaitGroup{}
	wg.Add(1)

	l.Infoln("starting the colletor consumer...")

	go func() {
		app.RunCollectorConsumer(l, ctx, wg)
	}()

	srv := &http.Server{
		Handler: utils.LogRequestHandler(l, r),
		Addr:    fmt.Sprintf("127.0.0.1:%d", conf.Port),
		// Good practice: enforce timeouts for servers you create!
		WriteTimeout: 6000 * time.Second,
		ReadTimeout:  6000 * time.Second,
	}

	l.WithField("port", conf.Port).Infoln("starting the server...")
	serverErr := make(chan error, 1)
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			serverErr <- err
		}
	}()

	term := make(chan os.Signal, 1)
	signal.Notify(term, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-term:
		l.Infoln("signal received")
	case err := <-serverErr:
		l.WithError(err).Fatalln("server failed")
	}

	l.Infoln("shutting down the server...")
	if err := srv.Shutdown(context.Background()); err != nil {
		l.WithError(err).Errorln("server shutdown failed")
	}

	l.Infoln("waiting for all goroutines to finish...")
	cancelFunc()
	wg.Wait()

	l.Infoln("closing all db connections...")
	db.Close()

	l.Infoln("goodbye")
}
