package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/Dri0m/flashpoint-analytics-server/utils"
	"github.com/gorilla/mux"
)

func (a *App) KeyAuth(key string, next func(http.ResponseWriter, *http.Request)) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		reqKey := r.Header.Get("Authorization")
		splitToken := strings.Split(reqKey, "Bearer ")

		if len(splitToken) != 2 {
			http.Error(w, "invalid auth", http.StatusUnauthorized)
			return
		}

		reqKey = splitToken[1]

		if len(reqKey) == 0 {
			http.Error(w, "missing auth", http.StatusUnauthorized)
			return
		}

		if reqKey == key {
			next(w, r)
			return
		}

		http.Error(w, "invalid auth", http.StatusUnauthorized)
	}
}

const ContextSessionID = "SessionID"

func (a *App) SessionAuth(next func(http.ResponseWriter, *http.Request)) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		vars := mux.Vars(r)
		sessionUUID := vars["uuid"]

		reqToken := r.Header.Get("Authorization")
		splitToken := strings.Split(reqToken, "Bearer ")

		if len(splitToken) != 2 {
			http.Error(w, "invalid auth", http.StatusUnauthorized)
			return
		}

		reqToken = splitToken[1]

		if len(reqToken) == 0 {
			http.Error(w, "missing auth", http.StatusUnauthorized)
			return
		}

		session, err := a.DAL.GetSessionCached(ctx, sessionUUID)
		if err != nil {
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}

		if reqToken == session.Token {
			r = r.WithContext(context.WithValue(r.Context(), ContextSessionID, session.ID))
			next(w, r)
			return
		}

		http.Error(w, "invalid auth", http.StatusUnauthorized)
	}
}

func (a *App) FillFakeDataHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	err := a.DAL.InsertFooData(ctx)

	if err != nil {
		a.l.Error(err)
		http.Error(w, err.Error(), 500)
		return
	}

	w.WriteHeader(http.StatusOK)
}

type InitSessionResponse struct {
	SessionUUID  string `json:"session_uuid"`
	SessionToken string `json:"session_token"`
}

func (a *App) InitSessionHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	vars := mux.Vars(r)
	userUUID := vars["uuid"]

	// If you are using strings that may be invalid, check that ip is not nil
	ip := net.ParseIP(utils.RequestGetRemoteAddress(r))
	record, err := a.mmdb.City(ip)
	if err != nil {
		a.l.Error(err)
		http.Error(w, err.Error(), 500)
		return
	}

	sessionUUID, sessionToken, err := a.DAL.CreateSession(ctx, userUUID, record.Country.IsoCode)
	if err != nil {
		a.l.Error(err)
		http.Error(w, err.Error(), 500)
		return
	}

	result := InitSessionResponse{
		SessionUUID:  *sessionUUID,
		SessionToken: *sessionToken,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

type CollectRequest struct {
	Category string `json:"category"`
	Key      string `json:"key"`
	Value    string `json:"value"`
}

func (c *CollectRequest) Validate() (err error) {
	if len(c.Category) == 0 {
		return fmt.Errorf("category cannot be empty")
	}
	if len(c.Key) == 0 {
		return fmt.Errorf("key cannot be empty")
	}
	return nil
}

type Entry struct {
	SessionID int64
	Category  string
	Key       string
	Value     string
	CreatedAt time.Time
}

func (a *App) CollectHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	sid := ctx.Value(ContextSessionID)
	if sid == nil {
		err := fmt.Errorf("no sessionID provided")
		a.l.Error(err)
		http.Error(w, err.Error(), 500)
		return
	}
	SessionID := sid.(int64)

	c := &CollectRequest{}
	err := utils.DecodeJSONBody(w, r, c)
	if err != nil {
		a.l.Error(err)
		http.Error(w, err.Error(), 500)
		return
	}

	if err := c.Validate(); err != nil {
		a.l.Error(err)
		http.Error(w, err.Error(), 500)
		return
	}

	entry := &Entry{
		SessionID: SessionID,
		Category:  c.Category,
		Key:       c.Key,
		Value:     c.Value,
		CreatedAt: time.Now(),
	}

	a.collectQueue <- entry
	a.announceCollectQueue()
	w.WriteHeader(http.StatusNoContent)
}

type CurrentlyOnlineRequest struct {
	From     uint64 `schema:"from"`
	To       uint64 `schema:"to"`
	Interval uint64 `schema:"interval"`
}

type CurrentlyOnlineResponse struct {
	Result []CurrentlyOnline `json:"result"`
}

func (a *App) GetCurrentlyOnlineHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	filter := &CurrentlyOnlineRequest{}

	if err := a.decoder.Decode(filter, r.URL.Query()); err != nil {
		a.l.Error(err)
		http.Error(w, err.Error(), 500)
		return
	}

	if filter.Interval == 0 {
		filter.Interval = 999999999
	}

	a.l.Debug(filter)

	result, err := a.DAL.GetCurrentlyOnline(ctx, filter)

	if err != nil {
		a.l.Error(err)
		http.Error(w, err.Error(), 500)
		return
	}

	response := CurrentlyOnlineResponse{
		Result: result,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}
