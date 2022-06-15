package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Dri0m/flashpoint-analytics-server/utils"
	"github.com/gorilla/mux"
	"github.com/joho/godotenv"
	"github.com/sirupsen/logrus"
)

func panicErr(l *logrus.Entry, err error) {
	if err != nil {
		l.Fatal(err)
	}
}

type App struct {
	DAL *DAL
	l   *logrus.Entry
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
