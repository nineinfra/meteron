// Meteron 主程序：模块化单体入口（Gateway + Management API + 全部后台 Worker）。
// 生产部署可按 cmd 拆分为独立进程（api-server / gateway / worker）。
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/meteron/meteron/internal/api"
	"github.com/meteron/meteron/internal/billing"
	"github.com/meteron/meteron/internal/config"
	"github.com/meteron/meteron/internal/gateway"
	"github.com/meteron/meteron/internal/ingest"
	"github.com/meteron/meteron/internal/ledger"
	"github.com/meteron/meteron/internal/meter"
	"github.com/meteron/meteron/internal/pricing"
	"github.com/meteron/meteron/internal/provider"
	"github.com/meteron/meteron/internal/recon"
	"github.com/meteron/meteron/internal/rbac"
	"github.com/meteron/meteron/internal/router"
	"github.com/meteron/meteron/internal/store"
	"github.com/meteron/meteron/internal/ui"
	"github.com/meteron/meteron/internal/worker"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(log)

	cfg, err := config.Load()
	if err != nil {
		log.Error("load config failed", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// ---- 基础设施 ----
	db, err := store.NewPG(ctx, cfg.PGDSN)
	if err != nil {
		log.Error("connect pg failed", "err", err)
		os.Exit(1)
	}
	defer db.Close()
	if err := store.MigratePG(ctx, db); err != nil {
		log.Error("migrate pg failed", "err", err)
		os.Exit(1)
	}
	log.Info("pg ready (migrations applied)")

	ch, err := store.NewClickHouse(ctx, cfg.ClickHouseDSN)
	if err != nil {
		log.Error("connect clickhouse failed", "err", err)
		os.Exit(1)
	}
	defer ch.Close()
	log.Info("clickhouse ready (ddl applied)")

	rds, err := store.NewRedis(ctx, cfg.RedisAddr)
	if err != nil {
		log.Error("connect redis failed", "err", err)
		os.Exit(1)
	}
	log.Info("redis ready")

	if err := store.EnsureKafkaTopic(cfg.KafkaBrokers, cfg.KafkaTopic); err != nil {
		log.Warn("ensure kafka topic failed (may auto-create on first write)", "err", err)
	}
	kafkaWriter := store.NewKafkaWriter(cfg.KafkaBrokers, cfg.KafkaTopic)
	defer kafkaWriter.Close()

	// ---- 领域服务 ----
	authSvc, err := rbac.NewService(db, cfg.AESKey)
	if err != nil {
		log.Error("init rbac failed", "err", err)
		os.Exit(1)
	}
	if _, created, err := authSvc.Bootstrap(ctx, cfg.BootstrapKey); err != nil {
		log.Error("bootstrap tenant failed", "err", err)
		os.Exit(1)
	} else if created {
		log.Info("bootstrap tenant created (admin key issued on first boot)")
	} else {
		log.Info("bootstrap tenant verified")
	}

	ledgerStore := ledger.NewStore(db)
	ledgerSvc := ledger.NewService(ledgerStore, rds, db, log)
	pricingEngine := pricing.NewEngine(db, log)
	routerEngine := router.NewEngine(db)

	providers, err := provider.NewRegistry(cfg.Providers, 120*time.Second)
	if err != nil {
		log.Error("init provider registry failed", "err", err)
		os.Exit(1)
	}
	producer := ingest.NewProducer(kafkaWriter, log)
	defer producer.Close()

	billingWorker := billing.NewWorker(db, ch, rds, cfg, log)
	reconWorker := recon.NewWorker(db, ch, rds, cfg, log)

	gw := gateway.NewHandler(authSvc, ledgerSvc, ledgerStore, routerEngine, pricingEngine,
		providers, producer, db, cfg, log)
	apiServer := api.NewServer(db, ch, authSvc, ledgerSvc, pricingEngine, producer,
		billingWorker, cfg, log)

	// ---- HTTP 路由 ----
	mux := http.NewServeMux()
	mux.Handle("/v1/chat/completions", gw)
	mux.Handle("/api/v1/", apiServer.Handler())
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	// Web 控制台（内嵌静态资源，精确路径优先级高于 "/"）
	mux.Handle("/", ui.Handler())

	srv := &http.Server{
		Addr:              cfg.Server.Addr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	// ---- 后台 Worker ----
	sinkWorker := meter.NewSinkWorker(ch, cfg.KafkaBrokers, cfg.KafkaTopic, log)
	meterWorker := meter.NewMeterWorker(ch, cfg.KafkaBrokers, cfg.KafkaTopic, log)
	scheduledWorker := worker.NewScheduledWorker(db, rds, ledgerSvc, cfg, log)

	go func() {
		if err := sinkWorker.Run(ctx); err != nil {
			log.Error("sink worker exited", "err", err)
		}
	}()
	go func() {
		if err := meterWorker.Run(ctx); err != nil {
			log.Error("meter worker exited", "err", err)
		}
	}()
	go func() {
		if err := scheduledWorker.Run(ctx); err != nil {
			log.Error("scheduled worker exited", "err", err)
		}
	}()
	go func() {
		if err := billingWorker.RunDailyLoop(ctx); err != nil {
			log.Error("billing loop exited", "err", err)
		}
	}()
	go func() {
		if err := reconWorker.Run(ctx); err != nil {
			log.Error("recon worker exited", "err", err)
		}
	}()

	// ---- 启动 HTTP ----
	go func() {
		log.Info("meteron listening", "addr", cfg.Server.Addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("http server error", "err", err)
			stop()
		}
	}()

	<-ctx.Done()
	log.Info("shutting down...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Warn("http shutdown error", "err", err)
	}
	log.Info("meteron stopped")
}
