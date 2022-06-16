package utils

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/felixge/httpsnoop"
	"github.com/golang/gddo/httputil/header"
	"github.com/sirupsen/logrus"
	"gopkg.in/natefinch/lumberjack.v2"
)

func InitLogger() *logrus.Logger {
	mw := io.MultiWriter(os.Stdout, &lumberjack.Logger{
		Filename:   "log.log",
		MaxSize:    500, // megabytes
		MaxAge:     0,   //days
		MaxBackups: 0,
		Compress:   true,
	})
	// mw := io.MultiWriter(os.Stdout)
	l := logrus.New()
	l.SetFormatter(&logrus.TextFormatter{
		DisableColors:   true,
		FullTimestamp:   true,
		TimestampFormat: time.RFC3339Nano,
	})
	l.SetOutput(mw)
	l.SetLevel(logrus.TraceLevel)
	l.SetReportCaller(true)
	return l
}

func Exists(name string) (bool, error) {
	_, err := os.Stat(name)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return false, err
}

// https://presstige.io/p/Logging-HTTP-requests-in-Go-233de7fe59a747078b35b82a1b035d36

// HTTPReqInfo is HTTP request info
type HTTPReqInfo struct {
	// GET etc.
	method  string
	uri     string
	referer string
	ipaddr  string

	code int

	size int64

	duration  time.Duration
	userAgent string
}

// LogRequestHandler is logging handler
func LogRequestHandler(l *logrus.Entry, h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ri := &HTTPReqInfo{
			method:    r.Method,
			uri:       r.URL.String(),
			referer:   r.Header.Get("Referer"),
			userAgent: r.Header.Get("User-Agent"),
		}

		ri.ipaddr = RequestGetRemoteAddress(r)

		// this runs handler h and captures information about
		// HTTP request
		m := httpsnoop.CaptureMetrics(h, w, r)

		ri.code = m.Code
		ri.size = m.Written
		ri.duration = m.Duration
		l.WithFields(logrus.Fields{"method": ri.method, "ip": ri.ipaddr, "uri": ri.uri, "statusCode": ri.code, "size": ri.size, "duration_ns": fmt.Sprintf("%d", ri.duration.Nanoseconds()), "userAgent": ri.userAgent}).Debug("request captured")
	})
}

// Request.RemoteAddress contains port, which we want to remove i.e.:
// "[::1]:58292" => "[::1]"
func ipAddrFromRemoteAddr(s string) string {
	idx := strings.LastIndex(s, ":")
	if idx == -1 {
		return s
	}
	return s[:idx]
}

// RequestGetRemoteAddress returns ip address of the client making the request,
// taking into account http proxies
func RequestGetRemoteAddress(r *http.Request) string {
	hdr := r.Header
	hdrRealIP := hdr.Get("X-Real-Ip")
	hdrForwardedFor := hdr.Get("X-Forwarded-For")
	if hdrRealIP == "" && hdrForwardedFor == "" {
		return ipAddrFromRemoteAddr(r.RemoteAddr)
	}
	if hdrForwardedFor != "" {
		// X-Forwarded-For is potentially a list of addresses separated with ","
		parts := strings.Split(hdrForwardedFor, ",")
		for i, p := range parts {
			parts[i] = strings.TrimSpace(p)
		}
		// TODO: should return first non-local address
		return parts[0]
	}
	return hdrRealIP
}

var src = rand.NewSource(time.Now().UnixNano())

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

func RandomString(n int) string {
	b := make([]byte, n)
	// A src.Int63() generates 63 random bits, enough for letterIdxMax characters!
	for i, cache, remain := n-1, src.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return string(b)
}

// https://www.alexedwards.net/blog/how-to-properly-parse-a-json-request-body

// MalformedRequest is MalformedRequest
type MalformedRequest struct {
	Status int
	Msg    string
}

func (mr *MalformedRequest) Error() string {
	return mr.Msg
}

// DecodeJSONBody parses and verifies JSON body
func DecodeJSONBody(w http.ResponseWriter, r *http.Request, dst interface{}) error {
	if r.Header.Get("Content-Type") != "" {
		value, _ := header.ParseValueAndParams(r.Header, "Content-Type")
		if value != "application/json" {
			msg := "Content-Type header is not application/json"
			return &MalformedRequest{Status: http.StatusUnsupportedMediaType, Msg: msg}
		}
	}
	const maxSize = 1024 * 1024 * 1
	r.Body = http.MaxBytesReader(w, r.Body, maxSize)

	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()

	err := dec.Decode(&dst)
	if err != nil {
		var syntaxError *json.SyntaxError
		var unmarshalTypeError *json.UnmarshalTypeError

		switch {
		case errors.As(err, &syntaxError):
			msg := fmt.Sprintf("Request body contains badly-formed JSON (at position %d)", syntaxError.Offset)
			return &MalformedRequest{Status: http.StatusBadRequest, Msg: msg}

		case errors.Is(err, io.ErrUnexpectedEOF):
			msg := fmt.Sprintf("Request body contains badly-formed JSON")
			return &MalformedRequest{Status: http.StatusBadRequest, Msg: msg}

		case errors.As(err, &unmarshalTypeError):
			msg := fmt.Sprintf("Request body contains an invalid value for the %q field (at position %d)", unmarshalTypeError.Field, unmarshalTypeError.Offset)
			return &MalformedRequest{Status: http.StatusBadRequest, Msg: msg}

		case strings.HasPrefix(err.Error(), "json: unknown field "):
			fieldName := strings.TrimPrefix(err.Error(), "json: unknown field ")
			msg := fmt.Sprintf("Request body contains unknown field %s", fieldName)
			return &MalformedRequest{Status: http.StatusBadRequest, Msg: msg}

		case errors.Is(err, io.EOF):
			msg := "Request body must not be empty"
			return &MalformedRequest{Status: http.StatusBadRequest, Msg: msg}

		case err.Error() == "http: request body too large":
			msg := "Request body must not be larger than 1MB"
			return &MalformedRequest{Status: http.StatusRequestEntityTooLarge, Msg: msg}

		default:
			return err
		}
	}

	err = dec.Decode(&struct{}{})
	if err != io.EOF {
		msg := "Request body must only contain a single JSON object"
		return &MalformedRequest{Status: http.StatusBadRequest, Msg: msg}
	}

	return nil
}

// NewBucketLimiter creates a ticker channel that fills a bucket with one token every d and has a given capacity for burst usage
func NewBucketLimiter(d time.Duration, capacity int) (chan bool, *time.Ticker) {
	bucket := make(chan bool, capacity)
	ticker := time.NewTicker(d)
	go func() {
		for {
			<-ticker.C
			bucket <- true
		}
	}()
	return bucket, ticker
}
