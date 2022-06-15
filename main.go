package main

import (
	"database/sql"
	"fmt"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/joho/godotenv"
	"github.com/sirupsen/logrus"
)

var l *logrus.Logger

// OpenDB opens DAL or panics
func OpenDB(conf *Config) *sql.DB {
	user := conf.DBUser
	pass := conf.DBPassword
	ip := conf.DBIP
	port := conf.DBPort
	dbName := conf.DBName

	db, err := sql.Open("mysql",
		fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?multiStatements=true", user, pass, ip, port, dbName))
	if err != nil {
		l.Fatal(err)
	}

	return db
}

func main() {
	err := godotenv.Load()
	if err != nil {
		panic(err)
	}

	l = initLogger()
	conf := GetConfig()
	
	db := OpenDB(conf)
	defer db.Close()

	r := mux.NewRouter()
	r.HandleFunc("/game/{uuid}/worky", workyHandler).Methods("POST")
	http.Handle("/", r)

	srv := &http.Server{
		Handler: LogRequestHandler(r),
		Addr:    fmt.Sprintf("127.0.0.1:%d", conf.Port),
		// Good practice: enforce timeouts for servers you create!
		WriteTimeout: 60 * time.Second,
		ReadTimeout:  60 * time.Second,
	}

	l.Fatal(srv.ListenAndServe())
}

func workyHandler(w http.ResponseWriter, r *http.Request) {
	//vars := mux.Vars(r)
	//uuid := vars["uuid"]

	w.WriteHeader(http.StatusOK)
}
