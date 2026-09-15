// Meteron 主程序：模块化单体入口（Gateway + Management API + 全部后台 Worker）。
// 生产部署可按 cmd 拆分为独立进程（api-server / gateway / worker）。
package main

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/meteron/meteron/internal/api"
	"github.com/meteron/meteron/internal/billing"
	"github.com/meteron/meteron/internal/config"
	"github.com/meteron/meteron/internal/gateway"
	"github.com/meteron/meteron/internal/health"
	"github.com/meteron/meteron/internal/ingest"
	"github.com/meteron/meteron/internal/ledger"
	"github.com/meteron/meteron/internal/meter"
	"github.com/meteron/meteron/internal/pricing"
	"github.com/meteron/meteron/internal/provider"
	"github.com/meteron/meteron/internal/ratelimit"
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
	// auto 路由的渠道分级反查（provider.cost_tier）
	routerEngine.SetLookup(providers)
	producer := ingest.NewProducer(kafkaWriter, ingest.NewPGOutboxStore(db), log)
	defer producer.Close()

	billingWorker := billing.NewWorker(db, ch, rds, cfg, log)
	reconWorker := recon.NewWorker(db, ch, rds, cfg, log)

	// 渠道健康跟踪 + 双重限流（客户维度 / 上游 Provider 账号维度）
	healthTracker := health.NewTracker(cfg.Health.Window, cfg.Health.ErrorRateThreshold, cfg.Health.MinSamples, log)
	limiter := ratelimit.NewRedisLimiter(rds.Client)

	gw := gateway.NewHandler(authSvc, ledgerSvc, ledgerStore, routerEngine, pricingEngine,
		providers, producer, db, cfg, log, healthTracker, limiter)
	apiServer := api.NewServer(db, ch, authSvc, ledgerSvc, pricingEngine, producer,
		billingWorker, cfg, log, healthTracker, gw)

	// 渠道主动探测（连通性检查，不消耗上游 token；interval<=0 时不启动）
	probeTargets := make([]health.ProbeTarget, 0, len(cfg.Providers))
	for _, p := range cfg.Providers {
		probeTargets = append(probeTargets, health.ProbeTarget{Name: p.Name, BaseURL: p.BaseURL})
	}
	prober := health.NewProber(healthTracker, probeTargets, cfg.Health.ProbeInterval, 10*time.Second, log)
	go func() {
		prober.Run(ctx)
	}()

	// ---- HTTP 路由（API 与 Web 控制台分离端点）----
	// API 端点（默认 :8080）：OpenAI 兼容网关 + 管理 API + 运维探针
	apiMux := http.NewServeMux()
	apiMux.Handle("/v1/chat/completions", gw)
	apiMux.Handle("/api/v1/", apiServer.Handler())
	apiMux.Handle("/metrics", promhttp.Handler())
	apiMux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	srv := &http.Server{
		Addr:              cfg.Server.Addr,
		Handler:           apiMux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	// Console 端点（默认 :9080）：Web UI 静态资源 + 反向代理 /api、/v1 到 API 端点
	// （同源代理，浏览器无需 CORS；控制台可只在内网暴露，API 端点独立对外）
	var consoleSrv *http.Server
	if cfg.Server.ConsoleAddr != "" {
		consoleSrv = newConsoleServer(cfg.Server.Addr, cfg.Server.ConsoleAddr, log)
	}

	// ---- 后台 Worker ----
	sinkWorker := meter.NewSinkWorker(ch, cfg.KafkaBrokers, cfg.KafkaTopic, log)
	meterWorker := meter.NewMeterWorker(ch, cfg.KafkaBrokers, cfg.KafkaTopic, log)
	scheduledWorker := worker.NewScheduledWorker(db, rds, ledgerSvc, cfg, log)

	go func() {
		if err := sinkWorker.Run(ctx); err != nil {
			// 计量落盘是核心链路：Worker 停止后 Kafka lag 会静默增长且无告警，
			// 必须让进程退出以触发重启/告警（未提交 offset，重放幂等）。
			log.Error("sink worker exited, shutting down process", "err", err)
			stop()
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
		log.Info("meteron api listening", "addr", cfg.Server.Addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("http server error", "err", err)
			stop()
		}
	}()
	if consoleSrv != nil {
		go func() {
			log.Info("meteron console listening", "addr", cfg.Server.ConsoleAddr)
			if err := consoleSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Error("console server error", "err", err)
			}
		}()
	}

	<-ctx.Done()
	log.Info("shutting down...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Warn("http shutdown error", "err", err)
	}
	if consoleSrv != nil {
		if err := consoleSrv.Shutdown(shutdownCtx); err != nil {
			log.Warn("console shutdown error", "err", err)
		}
	}
	log.Info("meteron stopped")
}

// newConsoleServer 构建 Web 控制台服务：静态 UI + /api、/v1 反向代理到 API 端点。
func newConsoleServer(apiAddr, consoleAddr string, log *slog.Logger) *http.Server {
	// API 地址统一补全为绝对 URL（":8080" → "http://127.0.0.1:8080"）
	if !strings.Contains(apiAddr, "://") {
		apiAddr = "http://" + strings.TrimPrefix(apiAddr, "http://")
		if strings.HasPrefix(apiAddr, "http://:") {
			apiAddr = "http://127.0.0.1" + strings.TrimPrefix(apiAddr, "http://")
		}
	}
	origin, err := url.Parse(apiAddr)
	if err != nil {
		log.Error("console proxy target invalid", "target", apiAddr, "err", err)
		return nil
	}
	proxy := httputil.NewSingleHostReverseProxy(origin)

	mux := http.NewServeMux()
	mux.Handle("/api/", proxy)
	mux.Handle("/v1/", proxy)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.Handle("/", ui.Handler())

	return &http.Server{
		Addr:              consoleAddr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}
}
