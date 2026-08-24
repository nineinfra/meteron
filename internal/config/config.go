// Package config 定义 Meteron 全部配置项。
// 优先级：默认值 < YAML 配置文件 < 环境变量（METERON_*）。
package config

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/shopspring/decimal"
	"gopkg.in/yaml.v3"
)

type ProviderConfig struct {
	Name     string   `yaml:"name"`      // provider 名称，route_rules.target_provider 引用
	Type     string   `yaml:"type"`      // openai | openai-compatible | local | anthropic | mock
	BaseURL  string   `yaml:"base_url"`
	APIKey   string   `yaml:"api_key"`   // 支持 ${ENV_VAR} 展开；local 类型可留空
	Models   []string `yaml:"models"`    // 该 provider 承载的模型列表（无路由规则时按 model 反查 provider）
	CostTier string   `yaml:"cost_tier"` // economy | standard | premium：成本分级（auto 路由按此分级选择渠道）
}

// RateLimit 双重限流配置（参考 sub2api：客户维度 + 上游 Provider 账号维度）。
// 0 表示不限制对应维度。
type RateLimit struct {
	CustomerRPM          int64 `yaml:"customer_rpm"`           // 客户维度每分钟请求数
	CustomerTPM          int64 `yaml:"customer_tpm"`           // 客户维度每分钟 token 数（按请求 input+max_tokens 估算）
	CustomerMaxConcurr   int64 `yaml:"customer_max_concurrent"` // 客户维度最大并发
	ProviderMaxConcurr   int64 `yaml:"provider_max_concurrent"` // 上游 Provider 渠道维度最大并发（防止打爆上游账号）
}

// Health 渠道健康探测配置（参考 new-api 渠道管理）。
type Health struct {
	ProbeInterval     time.Duration `yaml:"probe_interval"`      // 主动探测间隔（连通性检查，0=关闭主动探测）
	Window            time.Duration `yaml:"window"`              // 错误率统计滑动窗口
	ErrorRateThreshold float64      `yaml:"error_rate_threshold"` // 窗口内错误率超过该阈值自动禁用（0~1）
	MinSamples         int64        `yaml:"min_samples"`         // 触发自动禁用的最小样本数（防小样本误杀）
}

// Security 安全配置。
type Security struct {
	// TrustedProxies 可信反向代理地址（单 IP 或 CIDR）。
	// 仅当请求直连对端命中该列表时才解析 X-Forwarded-For 提取真实客户端 IP
	// （从右向左跳过可信代理段）；对端不可信时忽略 XFF——XFF 可被任意客户端
	// 伪造，直接取首段会被密钥 IP 白名单绕过。
	// 默认信任本机回环（内置控制台反代的默认部署形态）；
	// 直接对外暴露 API 端点时应清空（env 设为 none）。
	TrustedProxies []string `yaml:"trusted_proxies"`
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
		Addr        string `yaml:"addr"`         // API/网关端点 ":9080"（/v1 /api/v1 /metrics）
		ConsoleAddr string `yaml:"console_addr"` // Web 控制台端点 ":8080"（静态 UI + 反代 API）
	} `yaml:"server"`
	PGDSN         string           `yaml:"pg_dsn"`
	ClickHouseDSN string           `yaml:"clickhouse_dsn"`
	KafkaBrokers  []string         `yaml:"kafka_brokers"`
	KafkaTopic    string           `yaml:"kafka_topic"`
	RedisAddr     string           `yaml:"redis_addr"`
	AESKey        string           `yaml:"aes_key"`           // 32字节 hex，API Key 哈希（HMAC-SHA256）的 pepper
	BootstrapKey  string           `yaml:"bootstrap_api_key"` // 首次启动引导 API Key（明文，仅用于初始化）
	// InsecureDefaults 显式允许使用源码内置默认密钥（仅限本地开发环境）。
	// 默认 false：内置默认 aes_key/bootstrap key 已随源码公开，任何可达实例都可
	// 凭公开凭据获得 admin 权限（CWE-798），生产部署必须替换两者。
	InsecureDefaults bool             `yaml:"insecure_defaults"`
	Providers        []ProviderConfig `yaml:"providers"`
	Business         Business         `yaml:"business"`
	RateLimit        RateLimit        `yaml:"rate_limit"`
	Health           Health           `yaml:"health"`
	Security         Security         `yaml:"security"`
}

// 内置默认密钥（源码公开值，validate 拦截 + insecure_defaults 显式豁免）
const (
	defaultAESKeyHex    = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
	defaultBootstrapKey = "mk_meteron_bootstrap_dev_key"
)

func Default() *Config {
	c := &Config{}
	c.Server.Addr = ":9080"
	c.Server.ConsoleAddr = ":8080"
	c.PGDSN = "host=localhost port=5432 user=meteron password=meteron123 dbname=meteron sslmode=disable"
	c.ClickHouseDSN = "clickhouse://meteron:meteron123@localhost:9000/meteron"
	c.KafkaBrokers = []string{"localhost:9092"}
	c.KafkaTopic = "meteron.meter-events"
	c.RedisAddr = "localhost:6379"
	c.AESKey = defaultAESKeyHex
	c.BootstrapKey = defaultBootstrapKey
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
	c.RateLimit.CustomerRPM = 0
	c.RateLimit.CustomerTPM = 0
	c.RateLimit.CustomerMaxConcurr = 0
	c.RateLimit.ProviderMaxConcurr = 0
	c.Health.ProbeInterval = 60 * time.Second
	c.Health.Window = 5 * time.Minute
	c.Health.ErrorRateThreshold = 0.5
	c.Health.MinSamples = 10
	// 默认仅信任本机回环（内置控制台反代默认部署形态）；直接暴露 API 端点时清空
	c.Security.TrustedProxies = []string{"127.0.0.1", "::1"}
	c.Providers = []ProviderConfig{
		// 默认模型：本机 Ollama 的 qwen3.5:9b（OpenAI 兼容端点，api_key 留空）。
		// 容器内经 host.docker.internal 访问宿主机 Ollama；宿主机直跑时由 config.yaml 覆盖为 localhost。
		{Name: "ollama", Type: "local", BaseURL: "http://host.docker.internal:11434/v1", Models: []string{"qwen3.5:9b"}, CostTier: "economy"},
		// mock：无外部依赖的验证用 provider（verify.sh 使用）
		{Name: "mock", Type: "mock", BaseURL: "", APIKey: "", Models: []string{"mock-chat", "mock-cheap"}, CostTier: "economy"},
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
	data, err := os.ReadFile(path)
	switch {
	case err == nil:
		if err := yaml.Unmarshal(expandEnv(data), c); err != nil {
			return nil, fmt.Errorf("parse config %s: %w", path, err)
		}
	case errors.Is(err, fs.ErrNotExist):
		// 配置文件不存在：维持"存在才加载"语义（纯 env 部署形态）
	default:
		// 路径拼错/权限等读取错误：静默回退默认值会让服务带着公开的默认
		// 密钥与默认中间件地址启动且无任何提示，必须直接失败
		return nil, fmt.Errorf("read config %s: %w", path, err)
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
	set("METERON_INSECURE_DEFAULTS", func(v string) {
		c.InsecureDefaults = v == "true" || v == "1"
	})
	set("METERON_SERVER_ADDR", func(v string) { c.Server.Addr = v })
	set("METERON_CONSOLE_ADDR", func(v string) { c.Server.ConsoleAddr = v })
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
	setInt := func(k string, fn func(int64)) {
		set(k, func(v string) {
			if n, err := strconv.ParseInt(v, 10, 64); err == nil {
				fn(n)
			}
		})
	}
	setInt("METERON_RATE_LIMIT_CUSTOMER_RPM", func(n int64) { c.RateLimit.CustomerRPM = n })
	setInt("METERON_RATE_LIMIT_CUSTOMER_TPM", func(n int64) { c.RateLimit.CustomerTPM = n })
	setInt("METERON_RATE_LIMIT_CUSTOMER_MAX_CONCURRENT", func(n int64) { c.RateLimit.CustomerMaxConcurr = n })
	setInt("METERON_RATE_LIMIT_PROVIDER_MAX_CONCURRENT", func(n int64) { c.RateLimit.ProviderMaxConcurr = n })
	setDuration("METERON_HEALTH_PROBE_INTERVAL", func(d time.Duration) { c.Health.ProbeInterval = d })
	setDuration("METERON_HEALTH_WINDOW", func(d time.Duration) { c.Health.Window = d })
	set("METERON_HEALTH_ERROR_RATE_THRESHOLD", func(v string) {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			c.Health.ErrorRateThreshold = f
		}
	})
	setInt("METERON_HEALTH_MIN_SAMPLES", func(n int64) { c.Health.MinSamples = n })
	// 可信代理列表：逗号分隔 IP/CIDR；none=清空（直接对外暴露 API 端点时使用）
	set("METERON_TRUSTED_PROXIES", func(v string) {
		if v == "none" {
			c.Security.TrustedProxies = nil
			return
		}
		c.Security.TrustedProxies = strings.Split(v, ",")
	})
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
	// 内置默认密钥保护（CWE-798）：默认 aes_key / bootstrap key 已随源码公开，
	// 任何可达实例都可凭公开凭据获得 admin 权限（含密钥管理、账单、调试沙盒）。
	// 使用默认值必须显式声明 insecure_defaults（本地开发）；生产部署务必替换两者。
	if (c.AESKey == defaultAESKeyHex || c.BootstrapKey == defaultBootstrapKey) && !c.InsecureDefaults {
		return fmt.Errorf("检测到源码内置默认密钥（aes_key/bootstrap_api_key），存在公开凭据风险：" +
			"请通过 METERON_AES_KEY / METERON_BOOTSTRAP_API_KEY（或 yaml）替换；" +
			"本地开发可显式设置 insecure_defaults: true 或 METERON_INSECURE_DEFAULTS=true 豁免")
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
	if c.Health.Window <= 0 {
		c.Health.Window = 5 * time.Minute
	}
	if c.Health.ErrorRateThreshold <= 0 || c.Health.ErrorRateThreshold > 1 {
		c.Health.ErrorRateThreshold = 0.5
	}
	if c.Health.MinSamples <= 0 {
		c.Health.MinSamples = 10
	}
	// 限流配置不允许负值（负值会被 Redis 比较逻辑误判为超限）
	if c.RateLimit.CustomerRPM < 0 || c.RateLimit.CustomerTPM < 0 ||
		c.RateLimit.CustomerMaxConcurr < 0 || c.RateLimit.ProviderMaxConcurr < 0 {
		return fmt.Errorf("rate_limit 配置不允许负值")
	}
	// cost_tier 只允许三个已知分级 + 空值（未分级渠道不参与 auto 成本路由）
	seen := map[string]bool{}
	for _, p := range c.Providers {
		switch p.CostTier {
		case "", "economy", "standard", "premium":
		default:
			return fmt.Errorf("provider %s: cost_tier 非法值 %q（允许 economy/standard/premium）", p.Name, p.CostTier)
		}
		if seen[p.Name] {
			return fmt.Errorf("provider 名称重复: %s", p.Name)
		}
		seen[p.Name] = true
	}
	return nil
}
