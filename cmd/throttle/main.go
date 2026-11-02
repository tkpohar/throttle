// Command throttle runs the rate-limiting decision service.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/tkpohar/throttle/internal/config"
	"github.com/tkpohar/throttle/internal/server"
	"github.com/tkpohar/throttle/pkg/limiter"
)

func main() {
	if err := run(); err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run() error {
	cfgPath := flag.String("config", envOr("THROTTLE_CONFIG", "config.json"), "path to JSON config")
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(log)

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}

	metrics := server.NewMetrics()

	local := limiter.NewMemory()
	defer local.Close()

	var (
		lim   limiter.Limiter = local
		ready func(context.Context) error
	)
	if cfg.Backend == "redis" {
		timeout := time.Duration(cfg.RedisTimeout)
		rdb := redis.NewClient(&redis.Options{
			Addr:         cfg.RedisAddr,
			DialTimeout:  time.Second,
			ReadTimeout:  timeout,
			WriteTimeout: timeout,
			PoolSize:     64,
		})
		defer rdb.Close()
		mode, _ := limiter.ParseFailureMode(cfg.FailureMode) // validated in config.Load
		lim = &limiter.Failover{
			Primary:  limiter.NewRedis(rdb),
			Mode:     mode,
			Fallback: local,
			OnError: func(err error) {
				metrics.IncBackendError()
				log.Warn("redis backend error; applying failure mode", "mode", mode, "err", err)
			},
		}
		ready = func(ctx context.Context) error { return rdb.Ping(ctx).Err() }
	}

	srv := &http.Server{
		Addr: cfg.ListenAddr,
		Handler: server.New(server.Options{
			Limiter: lim, Rules: cfg.RuleSet(), Metrics: metrics,
			Ready: ready, Logger: log,
		}).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	go func() {
		log.Info("throttle listening", "addr", cfg.ListenAddr, "backend", cfg.Backend,
			"failure_mode", cfg.FailureMode, "rules", len(cfg.Rules))
		errCh <- srv.ListenAndServe()
	}()

	select {
	case err := <-errCh:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	case <-ctx.Done():
		log.Info("shutting down")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		return srv.Shutdown(shutdownCtx)
	}
	return nil
}

func envOr(k, def string) string {
	if v, ok := os.LookupEnv(k); ok {
		return v
	}
	return def
}
