// result-collector is an HTTP daemon that accepts benchmark results and stores them in PostgreSQL.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"

	"github.com/himanshuplace/protocol_for_broadcast/pkg/collector"
)

func main() {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://benchmark:benchmark@localhost:5432/benchmark?sslmode=disable"
	}
	addr := os.Getenv("LISTEN_ADDR")
	if addr == "" {
		addr = ":8080"
	}

	logger, _ := zap.NewProduction()
	defer logger.Sync() //nolint:errcheck

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	col, err := collector.NewPostgresCollector(ctx, dsn)
	if err != nil {
		logger.Fatal("connect postgres", zap.Error(err))
	}
	defer col.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("POST /results", func(w http.ResponseWriter, r *http.Request) {
		var result collector.RunResult
		if err := json.NewDecoder(r.Body).Decode(&result); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := col.Store(r.Context(), &result); err != nil {
			logger.Error("store result", zap.Error(err))
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusCreated)
	})

	mux.HandleFunc("GET /results", func(w http.ResponseWriter, r *http.Request) {
		proto := r.URL.Query().Get("protocol")
		scenario := r.URL.Query().Get("scenario")
		results, err := col.List(r.Context(), proto, scenario, 200)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(results)
	})

	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "ok")
	})

	srv := &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	go func() {
		logger.Info("result-collector listening", zap.String("addr", addr))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("server error", zap.Error(err))
		}
	}()

	<-ctx.Done()
	shutCtx, shutCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutCancel()
	_ = srv.Shutdown(shutCtx)
}
