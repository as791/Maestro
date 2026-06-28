package main

import (
	"context"
	"errors"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/maestro-flink/maestro/control"
	"github.com/maestro-flink/maestro/internal/api"
	"github.com/maestro-flink/maestro/internal/auth"
	"github.com/maestro-flink/maestro/internal/config"
	"go.temporal.io/sdk/client"
)

func main() {
	cfg := config.Load()
	auth.Init(cfg.Auth)

	temporalClient, err := client.Dial(client.Options{
		HostPort:  cfg.Temporal.Address,
		Namespace: cfg.Temporal.Namespace,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer temporalClient.Close()

	controlService := control.NewService(temporalClient, cfg.Actor.TaskQueue, cfg.Actor.ActivityTaskQueue, cfg.Actor.ContinueAfter, cfg.Actor.Shards)
	server := &http.Server{
		Addr:              cfg.HTTP.Address,
		Handler:           api.New(controlService).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	go func() {
		slog.Info("control API listening", "address", cfg.HTTP.Address)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("control API failed", "error", err)
			os.Exit(1)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		slog.Error("control API shutdown failed", "error", err)
	}
}
