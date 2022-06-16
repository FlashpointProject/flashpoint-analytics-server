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
	"github.com/gorilla/schema"
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
	DAL                  *DAL
	l                    *logrus.Entry
	collectQueue         chan *Entry
	collectQueueNotEmpty chan bool
	decoder              *schema.Decoder
	mmdb                 *geoip2.Reader
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

	decoder := schema.NewDecoder()

	app := App{
		DAL: &DAL{
			pool:         db,
			sessionCache: NewSessionCache(),
		},
		l:                    l,
		collectQueue:         make(chan *Entry, 10000),
		collectQueueNotEmpty: make(chan bool),
		decoder:              decoder,
	}

	mmdb, err := geoip2.Open("GeoLite2-Country.mmdb")
	if err != nil {
		log.Fatal(err)
	}
	defer mmdb.Close()
	app.mmdb = mmdb

	r := mux.NewRouter()

	r.HandleFunc("/user/{uuid}/session", app.KeyAuth(conf.AccessKey, app.InitSessionHandler)).Methods("GET")
	r.HandleFunc("/session/{uuid}/entry", app.SessionAuth(app.CollectHandler)).Methods("POST")

	r.HandleFunc("/data/online", app.KeyAuth(conf.ReadKey, app.GetCurrentlyOnlineHandler)).Methods("GET")

	r.HandleFunc("/fill-fake-data", app.FillFakeDataHandler).Methods("GET")
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
	l.Infoln("signal received")

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
