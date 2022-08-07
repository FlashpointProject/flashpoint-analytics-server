package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net"
	"net/http"
	"os"
	"strconv"
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

type Event struct {
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

	event := &Event{
		SessionID: SessionID,
		Category:  c.Category,
		Key:       c.Key,
		Value:     c.Value,
		CreatedAt: time.Now(),
	}

	a.collectQueue <- event
	w.WriteHeader(http.StatusNoContent)
}

type CurrentlyOnlineRequest struct {
	From     uint64  `json:"from"`
	To       uint64  `json:"to"`
	Interval *uint64 `json:"interval"`
}

type CurrentlyOnlineResponse struct {
	Result []CurrentlyOnline `json:"result"`
}

func (a *App) GetCurrentlyOnlineHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	filter := &CurrentlyOnlineRequest{}
	err := utils.DecodeJSONBody(w, r, filter)
	if err != nil {
		a.l.Error(err)
		http.Error(w, err.Error(), 500)
		return
	}

	if filter.Interval == nil || *filter.Interval == 0 {
		var value uint64 = 999999999
		filter.Interval = &value
	}

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

type RawEventsRequest struct {
	From              uint64  `json:"from"`
	To                uint64  `json:"to"`
	SessionFrom       *uint64 `json:"session_from"`
	SessionTo         *uint64 `json:"session_to"`
	SessionCountryISO *string `json:"session_country_iso"`
	SessionUUID       *string `json:"session_uuid"`
	EventCategory     *string `json:"event_category"`
	EventKey          *string `json:"event_key"`
	EventValue        *string `json:"event_value"`
}

type RawEventsResponse struct {
	Result []RawEvent `json:"result"`
}

func (a *App) GetRawEventsHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	filter := &RawEventsRequest{}
	err := utils.DecodeJSONBody(w, r, filter)
	if err != nil {
		a.l.Error(err)
		http.Error(w, err.Error(), 500)
		return
	}

	result, err := a.DAL.GetRawEvents(ctx, filter)

	if err != nil {
		a.l.Error(err)
		http.Error(w, err.Error(), 500)
		return
	}

	response := RawEventsResponse{
		Result: result,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

type EventCountRequest struct {
	From          uint64  `json:"from"`
	To            uint64  `json:"to"`
	EventCategory *string `json:"event_category"`
	EventKey      *string `json:"event_key"`
	EventValue    *string `json:"event_value"`
}

type EventCountResponse struct {
	Result []EventCount `json:"result"`
}

func (a *App) GetEventCountHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	filter := &EventCountRequest{}
	err := utils.DecodeJSONBody(w, r, filter)
	if err != nil {
		a.l.Error(err)
		http.Error(w, err.Error(), 500)
		return
	}

	result, err := a.DAL.GetEventCount(ctx, filter)

	if err != nil {
		a.l.Error(err)
		http.Error(w, err.Error(), 500)
		return
	}

	response := EventCountResponse{
		Result: result,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

type IngestStuffFormat struct {
	UnixDate int64              `json:"unixDate"`
	Data     []IngestStuffEvent `json:"data"`
}

type IngestStuffEvent struct {
	EventCategory string `json:"event_category"`
	EventKey      string `json:"event_key"`
	EventValue    string `json:"event_value"`
	EventCount    string `json:"event_count"`
}

func (a *App) IngestStuffHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	const UUID = "00000000-0000-0000-0000-000000000000"
	const Country = "FPU"

	sessionUUID, _, err := a.DAL.CreateSession(ctx, UUID, Country)
	if err != nil {
		a.l.Error(err)
		http.Error(w, err.Error(), 500)
		return
	}

	session, err := a.DAL.GetSessionCached(ctx, *sessionUUID)
	if err != nil {
		a.l.Error(err)
		http.Error(w, err.Error(), 500)
		return
	}

	if err != nil {
		a.l.Error(err)
		http.Error(w, err.Error(), 500)
		return
	}

	files, err := os.ReadDir("./ingest")
	if err != nil {
		a.l.Error(err)
		http.Error(w, err.Error(), 500)
		return
	}

	for _, f := range files {
		if f.IsDir() {
			continue
		}
		a.l.Debugf("processing file %s", f.Name())

		err := func() error {
			jsonFile, err := os.Open(fmt.Sprintf("./ingest/%s", f.Name()))
			if err != nil {
				a.l.Error(err)
				return err
			}
			defer jsonFile.Close()

			bytes, err := ioutil.ReadAll(jsonFile)
			if err != nil {
				a.l.Error(err)
				return err
			}

			var data IngestStuffFormat

			err = json.Unmarshal(bytes, &data)
			if err != nil {
				a.l.Error(err)
				return err
			}

			for _, eventData := range data.Data {
				eventCount, err := strconv.Atoi(eventData.EventCount)
				if err != nil {
					a.l.Error(err)
					return err
				}

				// the data are clustered per day, let's explode it
				for i := 0; i < eventCount; i++ {
					randomSeconds := rand.Intn(86400)

					event := &Event{
						SessionID: session.ID,
						Category:  eventData.EventCategory,
						Key:       eventData.EventKey,
						Value:     eventData.EventValue,
						CreatedAt: time.Unix(data.UnixDate, 0).Add(time.Duration(randomSeconds) * time.Second),
					}

					a.collectQueue <- event
				}
			}

			return nil
		}()

		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
	}

	w.WriteHeader(http.StatusOK)
}
