package main

import (
	"ch-1/internal/apperrors"
	"ch-1/internal/consumer"
	"ch-1/internal/stats"
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	st := stats.New()

	// consumerErr receives the terminal error from the consumer goroutine
	// so that process termination is decided here, not inside consumer.Start.
	consumerErr := make(chan error, 1)
	go func() {
		consumerErr <- consumer.Start(ctx, st)
	}()

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
		Addr:         ":7001",
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	// shutdown runs the graceful stop sequence and is called from both
	// the OS signal path and the consumer-error path.
	shutdown := func(reason string) {
		log.Printf("shutting down: %s", reason)
		cancel() // stop the consumer goroutine

		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()

		// stop letting new requests in, finalize existing
		if err := server.Shutdown(shutdownCtx); err != nil {
			shutdownError := &apperrors.ShutdownError{Err: err}
			log.Printf("[%s] %v", shutdownError.Code(), shutdownError)
		}
	}

	done := make(chan struct{}) // empty signal channel
	go func() {
		defer close(done) // closing = broadcasting "I am done"
		select {
		case sig := <-quit:
			shutdown(sig.String()) // shutdown() runs to completion
		case err := <-consumerErr:
			if err != nil && !errors.Is(err, context.Canceled) {
				var appErr apperrors.AppError
				if errors.As(err, &appErr) {
					log.Printf("[%s] consumer failed: %v", appErr.Code(), appErr)
				} else {
					log.Printf("consumer failed: %v", err)
				}
			}
			shutdown("consumer exited")
		}
	}()

	log.Println("listening on :7001")
	if err := server.ListenAndServe(); err != http.ErrServerClosed {
		log.Fatal(err)
	}
	<-done // wait for shutdown to finish
}
