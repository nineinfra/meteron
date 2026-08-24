package config

import (
	"path/filepath"
	"strings"
	"testing"
)

// 内置默认密钥保护（CWE-798）：使用源码公开的默认 aes_key / bootstrap key
// 必须被 validate 拦截，除非显式声明 insecure_defaults（本地开发豁免）。

func TestValidateRejectsDefaultSecrets(t *testing.T) {
	c := Default() // 默认值即内置公开密钥
	if err := validate(c); err == nil {
		t.Fatal("default secrets must be rejected without insecure_defaults")
	} else if !strings.Contains(err.Error(), "默认密钥") {
		t.Fatalf("error should explain the fix: %v", err)
	}

	// 仅替换其一仍拒绝（两个默认值都是公开凭据）
	c2 := Default()
	c2.BootstrapKey = "mk_custom_key"
	if err := validate(c2); err == nil {
		t.Fatal("default aes_key alone must still be rejected")
	}
	c3 := Default()
	c3.AESKey = strings.Repeat("ab", 32)
	if err := validate(c3); err == nil {
		t.Fatal("default bootstrap key alone must still be rejected")
	}
}

func TestValidateAcceptsDefaultSecretsWithOptIn(t *testing.T) {
	c := Default()
	c.InsecureDefaults = true
	if err := validate(c); err != nil {
		t.Fatalf("explicit insecure_defaults must allow dev usage: %v", err)
	}
}

func TestValidateAcceptsCustomSecrets(t *testing.T) {
	c := Default()
	c.AESKey = strings.Repeat("ab", 32)
	c.BootstrapKey = "mk_custom_key"
	if err := validate(c); err != nil {
		t.Fatalf("custom secrets must always pass: %v", err)
	}
}

// provider cost_tier 校验：只允许 economy/standard/premium + 空值（未分级）。
func TestValidateProviderCostTier(t *testing.T) {
	newC := func(tier string) *Config {
		c := Default()
		c.AESKey = strings.Repeat("ab", 32)
		c.BootstrapKey = "mk_custom_key"
		c.Providers = append(c.Providers, ProviderConfig{Name: "p1", Type: "mock", CostTier: tier})
		return c
	}
	for _, tier := range []string{"", "economy", "standard", "premium"} {
		if err := validate(newC(tier)); err != nil {
			t.Errorf("cost_tier %q must be accepted: %v", tier, err)
		}
	}
	if err := validate(newC("cheap")); err == nil {
		t.Fatal("unknown cost_tier must be rejected")
	} else if !strings.Contains(err.Error(), "cost_tier") {
		t.Fatalf("error should mention cost_tier: %v", err)
	}
	// provider 名称重复必须拒绝（tier 反查/注册表依赖名称唯一）
	c := newC("")
	c.Providers = append(c.Providers, ProviderConfig{Name: "p1", Type: "mock"})
	if err := validate(c); err == nil || !strings.Contains(err.Error(), "重复") {
		t.Fatalf("duplicate provider name must be rejected: %v", err)
	}
}

// applyEnv 的 METERON_INSECURE_DEFAULTS 覆盖。
func TestApplyEnvInsecureDefaults(t *testing.T) {
	t.Setenv("METERON_INSECURE_DEFAULTS", "true")
	c := Default()
	applyEnv(c)
	if !c.InsecureDefaults {
		t.Fatal("METERON_INSECURE_DEFAULTS=true must set InsecureDefaults")
	}
}

// Load 对配置文件读取错误（非"文件不存在"）必须失败，
// 静默回退默认值会让服务带公开默认密钥启动。
func TestLoadFailsOnUnreadableConfig(t *testing.T) {
	dir := t.TempDir()
	// 目录作为文件路径传给 ReadFile → 读取错误（非 not exist）
	t.Setenv("METERON_CONFIG", filepath.Join(dir, "nope", ".."))
	if _, err := Load(); err == nil {
		t.Fatal("read error (non-ErrNotExist) must fail Load")
	}
}

// 纯 env 部署（无 config.yaml）+ 自定义密钥：正常加载。
func TestLoadEnvOnlyWithCustomSecrets(t *testing.T) {
	dir := t.TempDir() // 空目录：不存在 config.yaml
	t.Setenv("METERON_CONFIG", filepath.Join(dir, "config.yaml"))
	t.Setenv("METERON_AES_KEY", strings.Repeat("cd", 32))
	t.Setenv("METERON_BOOTSTRAP_API_KEY", "mk_custom_key")
	c, err := Load()
	if err != nil {
		t.Fatalf("env-only with custom secrets must load: %v", err)
	}
	if c.AESKey != strings.Repeat("cd", 32) || c.BootstrapKey != "mk_custom_key" {
		t.Fatalf("env override failed: aes=%q key=%q", c.AESKey, c.BootstrapKey)
	}
}
