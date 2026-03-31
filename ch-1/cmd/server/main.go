package main

import (
	"ch-1/internal/consumer"
	"ch-1/internal/stats"
	"context"

	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	// Easy run with blocking
	// ctx := context.Background()
	// consumer.Start(ctx)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	st := stats.New()

	log.Println(st)

	go consumer.Start(ctx, st)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(st.Snapshot())
	})

	server := &http.Server{
		Addr:         ":7000",
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-quit // block until signal received
		log.Println("shutting down...")
		cancel() // stop the consumer go-routine

		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		server.Shutdown(shutdownCtx) // drain in-flight HTTP requests
	}()

	log.Println("listening on :7000")
	if err := server.ListenAndServe(); err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
