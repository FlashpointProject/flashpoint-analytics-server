package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
)

func TestParseAnalyticsRange(t *testing.T) {
	to := uint64(time.Date(2026, 5, 27, 12, 0, 0, 0, time.UTC).Unix())

	tests := []struct {
		name         string
		rangeName    string
		wantDuration time.Duration
		wantBucket   time.Duration
	}{
		{name: "24 hours", rangeName: "24h", wantDuration: 24 * time.Hour, wantBucket: time.Hour},
		{name: "30 days", rangeName: "30d", wantDuration: 30 * 24 * time.Hour, wantBucket: 24 * time.Hour},
		{name: "year", rangeName: "year", wantDuration: 365 * 24 * time.Hour, wantBucket: 7 * 24 * time.Hour},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseAnalyticsRange(tt.rangeName, &to)
			if err != nil {
				t.Fatalf("parseAnalyticsRange returned error: %v", err)
			}
			if got.Duration != tt.wantDuration {
				t.Fatalf("duration = %s, want %s", got.Duration, tt.wantDuration)
			}
			if got.Bucket != tt.wantBucket {
				t.Fatalf("bucket = %s, want %s", got.Bucket, tt.wantBucket)
			}
			wantTo := time.Unix(int64(to), 0).UTC().Truncate(tt.wantBucket)
			if !got.To.Equal(wantTo) {
				t.Fatalf("to = %s, want %s", got.To, wantTo)
			}
			if !got.From.Equal(got.To.Add(-tt.wantDuration)) {
				t.Fatalf("from = %s, want %s", got.From, got.To.Add(-tt.wantDuration))
			}
		})
	}
}

func TestParseAnalyticsRangeRejectsInvalidRange(t *testing.T) {
	if _, err := parseAnalyticsRange("7d", nil); err == nil {
		t.Fatal("expected invalid range error")
	}
}

func TestChartHandlersRejectInvalidRangeBeforeDAL(t *testing.T) {
	app := &App{l: logrus.New().WithField("test", "chart-handlers")}

	tests := []struct {
		name    string
		handler http.HandlerFunc
		body    string
	}{
		{name: "active users", handler: app.GetActiveUsersChartHandler, body: `{"range":"7d"}`},
		{name: "active countries", handler: app.GetActiveCountriesHandler, body: `{"range":"7d"}`},
		{name: "top games", handler: app.GetTopGamesHandler, body: `{"range":"7d"}`},
		{name: "hardware trends", handler: app.GetHardwareTrendsHandler, body: `{"range":"7d"}`},
		{name: "activity heatmap", handler: app.GetActivityHeatmapHandler, body: `{"range":"7d"}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/data/chart/test", strings.NewReader(tt.body))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()

			tt.handler.ServeHTTP(rec, req)

			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
			}
			if !strings.Contains(rec.Body.String(), `"range"`) {
				t.Fatalf("expected range error, got %q", rec.Body.String())
			}
		})
	}
}

func TestActivityHeatmapHandlerRejects24hBeforeDAL(t *testing.T) {
	app := &App{l: logrus.New().WithField("test", "activity-heatmap")}
	req := httptest.NewRequest(http.MethodPost, "/data/chart/activity-heatmap", strings.NewReader(`{"range":"24h"}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	app.GetActivityHeatmapHandler(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"range"`) {
		t.Fatalf("expected range error, got %q", rec.Body.String())
	}
}

func TestTopGamesHandlerRejectsInvalidLimitBeforeDAL(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "zero", body: `{"range":"24h","limit":0}`},
		{name: "above max", body: `{"range":"24h","limit":101}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			app := &App{l: logrus.New().WithField("test", "top-games")}
			req := httptest.NewRequest(http.MethodPost, "/data/chart/top-games", strings.NewReader(tt.body))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()

			app.GetTopGamesHandler(rec, req)

			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
			}
			if !strings.Contains(rec.Body.String(), `"limit"`) {
				t.Fatalf("expected limit error, got %q", rec.Body.String())
			}
			if !strings.Contains(rec.Body.String(), "between 1 and 100") {
				t.Fatalf("expected max limit error, got %q", rec.Body.String())
			}
		})
	}
}

func TestTopGamesHandlerRejectsInvalidCountryBeforeDAL(t *testing.T) {
	app := &App{l: logrus.New().WithField("test", "top-games")}
	req := httptest.NewRequest(http.MethodPost, "/data/chart/top-games", strings.NewReader(`{"range":"24h","country_iso_code":"U5"}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	app.GetTopGamesHandler(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"country_iso_code"`) {
		t.Fatalf("expected country_iso_code error, got %q", rec.Body.String())
	}
}

func TestHardwareTrendsHandlerRejectsInvalidCountryBeforeDAL(t *testing.T) {
	app := &App{l: logrus.New().WithField("test", "hardware-trends")}
	req := httptest.NewRequest(http.MethodPost, "/data/chart/hardware-trends", strings.NewReader(`{"range":"24h","country_iso_code":"U5"}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	app.GetHardwareTrendsHandler(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"country_iso_code"`) {
		t.Fatalf("expected country_iso_code error, got %q", rec.Body.String())
	}
}

func TestHardwareTrendsHandlerRejectsInvalidHardwareKeyBeforeDAL(t *testing.T) {
	app := &App{l: logrus.New().WithField("test", "hardware-trends")}
	req := httptest.NewRequest(http.MethodPost, "/data/chart/hardware-trends", strings.NewReader(`{"range":"24h","hardware_key":"gpu"}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	app.GetHardwareTrendsHandler(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"hardware_key"`) {
		t.Fatalf("expected hardware_key error, got %q", rec.Body.String())
	}
}

func TestActivityHeatmapHandlerRejectsInvalidCountryBeforeDAL(t *testing.T) {
	app := &App{l: logrus.New().WithField("test", "activity-heatmap")}
	req := httptest.NewRequest(http.MethodPost, "/data/chart/activity-heatmap", strings.NewReader(`{"range":"30d","country_iso_code":"U5"}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	app.GetActivityHeatmapHandler(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"country_iso_code"`) {
		t.Fatalf("expected country_iso_code error, got %q", rec.Body.String())
	}
}

func TestNormalizeCountryISO(t *testing.T) {
	raw := " us "
	got, err := normalizeCountryISO(&raw)
	if err != nil {
		t.Fatalf("normalizeCountryISO returned error: %v", err)
	}
	if got == nil || *got != "US" {
		t.Fatalf("normalized country = %v, want US", got)
	}
}
