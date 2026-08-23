// Package config 定义 Meteron 全部配置项。
// 优先级：默认值 < YAML 配置文件 < 环境变量（METERON_*）。
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/shopspring/decimal"
	"gopkg.in/yaml.v3"
)

type ProviderConfig struct {
	Name    string   `yaml:"name"`    // provider 名称，route_rules.target_provider 引用
	Type    string   `yaml:"type"`    // openai | openai-compatible | local | anthropic | mock
	BaseURL string   `yaml:"base_url"`
	APIKey  string   `yaml:"api_key"` // 支持 ${ENV_VAR} 展开；local 类型可留空
	Models  []string `yaml:"models"`  // 该 provider 承载的模型列表（无路由规则时按 model 反查 provider）
}

type Business struct {
	EventLookbackWindowDays  int           `yaml:"event_lookback_window_days"`  // 晚到事件回溯窗口，默认34天
	ReserveDefaultTTLSeconds int           `yaml:"reserve_default_ttl_seconds"` // 预留默认TTL，默认60s
	EstimateUnitPrice        string        `yaml:"estimate_unit_price"`         // reserve 预估单价（每 token），默认 0.000002
	EstimateDefaultMaxTokens int           `yaml:"estimate_default_max_tokens"` // 预估输出 token 上限，默认1024
	EstimateCharsPerToken    int           `yaml:"estimate_chars_per_token"`    // 字符/token 换算系数，默认4
	ScheduledInterval        time.Duration `yaml:"scheduled_interval"`          // 过期预留扫描间隔
	ResyncInterval           time.Duration `yaml:"resync_interval"`             // Redis-PG 缓存漂移校正间隔（轻量对账）
	BillingInterval          time.Duration `yaml:"billing_interval"`            // BillingRun 自动调度间隔（0=仅手动）
	ReconLightInterval       time.Duration `yaml:"recon_light_interval"`        // 轻量对账间隔
	ReconDeepInterval        time.Duration `yaml:"recon_deep_interval"`         // 深度对账间隔
}

type Config struct {
	Server struct {
		Addr string `yaml:"addr"` // ":8080"
	} `yaml:"server"`
	PGDSN         string           `yaml:"pg_dsn"`
	ClickHouseDSN string           `yaml:"clickhouse_dsn"`
	KafkaBrokers  []string         `yaml:"kafka_brokers"`
	KafkaTopic    string           `yaml:"kafka_topic"`
	RedisAddr     string           `yaml:"redis_addr"`
	AESKey        string           `yaml:"aes_key"`           // 32字节 hex，API Key 加密主密钥
	BootstrapKey  string           `yaml:"bootstrap_api_key"` // 首次启动引导 API Key（明文，仅用于初始化）
	Providers     []ProviderConfig `yaml:"providers"`
	Business      Business         `yaml:"business"`
}

func Default() *Config {
	c := &Config{}
	c.Server.Addr = ":8080"
	c.PGDSN = "host=localhost port=5432 user=meteron password=meteron123 dbname=meteron sslmode=disable"
	c.ClickHouseDSN = "clickhouse://meteron:meteron123@localhost:9000/meteron"
	c.KafkaBrokers = []string{"localhost:9092"}
	c.KafkaTopic = "meteron.meter-events"
	c.RedisAddr = "localhost:6379"
	c.AESKey = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
	c.BootstrapKey = "mk_meteron_bootstrap_dev_key"
	c.Business.EventLookbackWindowDays = 34
	c.Business.ReserveDefaultTTLSeconds = 60
	c.Business.EstimateUnitPrice = "0.000002"
	c.Business.EstimateDefaultMaxTokens = 1024
	c.Business.EstimateCharsPerToken = 4
	c.Business.ScheduledInterval = 10 * time.Second
	c.Business.ResyncInterval = time.Hour
	c.Business.BillingInterval = 0
	c.Business.ReconLightInterval = time.Hour
	c.Business.ReconDeepInterval = 24 * time.Hour
	c.Providers = []ProviderConfig{
		{Name: "mock", Type: "mock", BaseURL: "", APIKey: "", Models: []string{"mock-chat", "mock-cheap"}},
	}
	return c
}

// Load 依次叠加：默认值 → YAML 文件（路径取 METERON_CONFIG，默认 ./config.yaml，存在才加载）→ 环境变量。
func Load() (*Config, error) {
	c := Default()

	path := os.Getenv("METERON_CONFIG")
	if path == "" {
		path = "config.yaml"
	}
	if data, err := os.ReadFile(path); err == nil {
		if err := yaml.Unmarshal(expandEnv(data), c); err != nil {
			return nil, fmt.Errorf("parse config %s: %w", path, err)
		}
	}

	applyEnv(c)
	if err := validate(c); err != nil {
		return nil, err
	}
	return c, nil
}

// expandEnv 展开 YAML 中的 ${VAR} 环境变量引用。
func expandEnv(data []byte) []byte {
	s := string(data)
	for {
		i := strings.Index(s, "${")
		if i < 0 {
			break
		}
		j := strings.Index(s[i:], "}")
		if j < 0 {
			break
		}
		j += i
		name := s[i+2 : j]
		s = s[:i] + os.Getenv(name) + s[j+1:]
	}
	return []byte(s)
}

func applyEnv(c *Config) {
	set := func(k string, fn func(string)) {
		if v, ok := os.LookupEnv(k); ok && v != "" {
			fn(v)
		}
	}
	set("METERON_PG_DSN", func(v string) { c.PGDSN = v })
	set("METERON_CLICKHOUSE_DSN", func(v string) { c.ClickHouseDSN = v })
	set("METERON_KAFKA_BROKERS", func(v string) { c.KafkaBrokers = strings.Split(v, ",") })
	set("METERON_KAFKA_TOPIC", func(v string) { c.KafkaTopic = v })
	set("METERON_REDIS_ADDR", func(v string) { c.RedisAddr = v })
	set("METERON_AES_KEY", func(v string) { c.AESKey = v })
	set("METERON_BOOTSTRAP_API_KEY", func(v string) { c.BootstrapKey = v })
	set("METERON_SERVER_ADDR", func(v string) { c.Server.Addr = v })
	set("METERON_EVENT_LOOKBACK_WINDOW_DAYS", func(v string) {
		if n, err := strconv.Atoi(v); err == nil {
			c.Business.EventLookbackWindowDays = n
		}
	})
	set("METERON_RESERVE_DEFAULT_TTL_SECONDS", func(v string) {
		if n, err := strconv.Atoi(v); err == nil {
			c.Business.ReserveDefaultTTLSeconds = n
		}
	})
	set("METERON_ESTIMATE_UNIT_PRICE", func(v string) { c.Business.EstimateUnitPrice = v })
	setDuration := func(k string, fn func(time.Duration)) {
		set(k, func(v string) {
			if d, err := time.ParseDuration(v); err == nil {
				fn(d)
			}
		})
	}
	setDuration("METERON_SCHEDULED_INTERVAL", func(d time.Duration) { c.Business.ScheduledInterval = d })
	setDuration("METERON_RESYNC_INTERVAL", func(d time.Duration) { c.Business.ResyncInterval = d })
	setDuration("METERON_BILLING_INTERVAL", func(d time.Duration) { c.Business.BillingInterval = d })
	setDuration("METERON_RECON_LIGHT_INTERVAL", func(d time.Duration) { c.Business.ReconLightInterval = d })
	setDuration("METERON_RECON_DEEP_INTERVAL", func(d time.Duration) { c.Business.ReconDeepInterval = d })
}

func validate(c *Config) error {
	if len(c.KafkaBrokers) == 0 {
		return fmt.Errorf("kafka brokers 未配置")
	}
	if c.BootstrapKey == "" {
		return fmt.Errorf("bootstrap api key 未配置")
	}
	if len(c.AESKey) != 64 {
		return fmt.Errorf("aes_key 必须为 64 位 hex 字符串（32字节）")
	}
	if c.Business.EventLookbackWindowDays <= 0 {
		c.Business.EventLookbackWindowDays = 34
	}
	if c.Business.ReserveDefaultTTLSeconds <= 0 {
		c.Business.ReserveDefaultTTLSeconds = 60
	}
	// 预估单价非法会导致 reserve 预留恒为 0，预算管控静默失效，必须启动期拦截
	if _, err := decimal.NewFromString(c.Business.EstimateUnitPrice); err != nil {
		return fmt.Errorf("estimate_unit_price 非法 decimal 值 %q: %v", c.Business.EstimateUnitPrice, err)
	}
	if c.Business.EstimateCharsPerToken <= 0 {
		c.Business.EstimateCharsPerToken = 4
	}
	if c.Business.EstimateDefaultMaxTokens < 0 {
		c.Business.EstimateDefaultMaxTokens = 1024
	}
	return nil
}
