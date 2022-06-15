package main

import (
	"net/http"
	"strings"
)

func CollectAuth(key string, next func(http.ResponseWriter, *http.Request)) func(http.ResponseWriter, *http.Request) {
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

func (h *App) FillFakeDataHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	err := h.DAL.InsertFooData(ctx)

	if err != nil {
		h.l.Error(err)
	}

	w.WriteHeader(http.StatusOK)
}

func (h *App) CollectHandler(w http.ResponseWriter, r *http.Request) {
	panic("not implemented")
}
