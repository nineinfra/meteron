package rbac

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"encoding/hex"
	"strings"
	"testing"
)

func testKey(t *testing.T) []byte {
	t.Helper()
	key, err := hex.DecodeString("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f")
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func newTestGCM(t *testing.T, key []byte) cipher.AEAD {
	t.Helper()
	block, err := aes.NewCipher(key)
	if err != nil {
		t.Fatal(err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		t.Fatal(err)
	}
	return gcm
}

// hashKey 核心安全属性：确定性（等值查找）、单向（不可逆）、pepper 依赖。
func TestHashKey(t *testing.T) {
	key := testKey(t)

	h1 := hashKey(key, "mk_secret_key_1")
	h2 := hashKey(key, "mk_secret_key_1")
	h3 := hashKey(key, "mk_secret_key_2")

	// 确定性：同一明文+主密钥 => 同一哈希（支持 DB 等值查找）
	if h1 != h2 {
		t.Fatalf("hashKey not deterministic: %s != %s", h1, h2)
	}
	// 不同明文 => 不同哈希
	if h1 == h3 {
		t.Fatalf("hashKey collision for different plaintexts")
	}
	// 输出为 64 位 hex（SHA-256），与旧格式长度可区分
	if len(h1) != 64 {
		t.Fatalf("hashKey length = %d, want 64", len(h1))
	}
	// 哈希不含明文信息
	if strings.Contains(h1, "mk_secret") {
		t.Fatalf("hash contains plaintext fragment")
	}

	// pepper 语义：主密钥不同 => 哈希不同（换 key 即全员失效）
	other := hashKey(append([]byte(nil), key...), "mk_secret_key_1")
	if other != h1 {
		t.Fatal("copied key should produce same hash")
	}
	key2 := append([]byte(nil), key...)
	key2[0] ^= 0xFF
	if hashKey(key2, "mk_secret_key_1") == h1 {
		t.Fatalf("different master key should produce different hash")
	}
}

// 旧格式（确定性 AES）可逆：持有主密钥即可还原明文——这是 P0 漏洞根因；
// 新格式单向，两者长度不同可区分。
func TestLegacyKeyHashReversibleAndDistinct(t *testing.T) {
	key := testKey(t)
	gcm := newTestGCM(t, key)
	plaintext := "mk_meteron_bootstrap_dev_key"

	legacy := legacyKeyHash(gcm, key, plaintext)
	hash := hashKey(key, plaintext)

	// 新旧格式输出必须不同（升级后同明文对应两个不同值）
	if legacy == hash {
		t.Fatalf("legacy hash equals new hash")
	}
	// 长度可区分：legacy = hex(12 nonce + 16 tag + len(pt))，new 恒为 64
	if len(legacy) != (12+16+len(plaintext))*2 {
		t.Fatalf("legacy hash length = %d, want %d", len(legacy), (12+16+len(plaintext))*2)
	}

	// 可逆性证明：legacy 可被解密还原明文（新格式则无对应逆运算）
	raw, err := hex.DecodeString(legacy)
	if err != nil {
		t.Fatal(err)
	}
	nonce, sealed := raw[:gcm.NonceSize()], raw[gcm.NonceSize():]
	decrypted, err := gcm.Open(nil, nonce, sealed, nil)
	if err != nil {
		t.Fatalf("legacy decrypt failed: %v", err)
	}
	if !bytes.Equal(decrypted, []byte(plaintext)) {
		t.Fatalf("legacy decrypt = %q, want %q", decrypted, plaintext)
	}
}

// 兼容迁移依赖：新旧格式对同一明文的计算互相独立、稳定，
// Bootstrap 的 UPDATE ... WHERE key_hash=$legacy 才能精确匹配存量行。
func TestLegacyKeyHashDeterministic(t *testing.T) {
	key := testKey(t)
	gcm := newTestGCM(t, key)
	if legacyKeyHash(gcm, key, "k1") != legacyKeyHash(gcm, key, "k1") {
		t.Fatalf("legacyKeyHash not deterministic")
	}
	if legacyKeyHash(gcm, key, "k1") == legacyKeyHash(gcm, key, "k2") {
		t.Fatalf("legacyKeyHash collision")
	}
}

// ---- IPAllowed：IP 白名单 ----

func TestIPAllowed(t *testing.T) {
	cases := []struct {
		name     string
		whitelist []string
		ip       string
		want     bool
	}{
		{"空白名单不限制", nil, "1.2.3.4", true},
		{"空白名单不限制(空数组)", []string{}, "8.8.8.8", true},
		{"单 IP 精确命中", []string{"1.2.3.4", "5.6.7.8"}, "1.2.3.4", true},
		{"单 IP 未命中", []string{"1.2.3.4"}, "1.2.3.5", false},
		{"CIDR 命中", []string{"10.0.0.0/8"}, "10.1.2.3", true},
		{"CIDR 未命中", []string{"10.0.0.0/8"}, "11.0.0.1", false},
		{"CIDR 边界(网络地址)", []string{"192.168.1.0/24"}, "192.168.1.0", true},
		{"CIDR 边界(广播地址)", []string{"192.168.1.0/24"}, "192.168.1.255", true},
		{"CIDR 边界外", []string{"192.168.1.0/24"}, "192.168.2.1", false},
		{"混合 单IP+CIDR", []string{"1.2.3.4", "10.0.0.0/8"}, "10.9.9.9", true},
		{"IPv6 精确命中", []string{"::1"}, "::1", true},
		{"IPv6 CIDR 命中", []string{"2001:db8::/32"}, "2001:db8::1", true},
		{"IPv6 未命中", []string{"2001:db8::/32"}, "2001:db9::1", false},
		{"IPv4 映射 IPv6 归一化", []string{"127.0.0.1"}, "127.0.0.1", true},
		{"非法来源 IP fail-closed", []string{"1.2.3.4"}, "not-an-ip", false},
		{"空来源 IP fail-closed", []string{"1.2.3.4"}, "", false},
		{"白名单含非法条目跳过", []string{"garbage", "1.2.3.4"}, "1.2.3.4", true},
		{"白名单含非法 CIDR 跳过", []string{"10.0.0.0/64", "1.2.3.4"}, "1.2.3.4", true},
		{"全部非法条目且不匹配", []string{"garbage", "10.0.0.0/64"}, "1.2.3.4", false},
		{"空白字符容忍", []string{" 1.2.3.4 "}, "1.2.3.4", true},
		{"IPv4-mapped IPv6 解析为 IPv4 命中", []string{"1.2.3.4"}, "::ffff:1.2.3.4", true},
	}
	for _, c := range cases {
		if got := IPAllowed(c.whitelist, c.ip); got != c.want {
			t.Errorf("%s: IPAllowed(%v, %q) = %v, want %v", c.name, c.whitelist, c.ip, got, c.want)
		}
	}
}

// IPAllowed fail-closed 语义：有白名单 + 无法解析来源 => 拒绝
func TestIPAllowedFailClosed(t *testing.T) {
	if IPAllowed([]string{"0.0.0.0/0"}, "") {
		t.Fatal("empty remote IP with non-empty whitelist must be rejected")
	}
}

// ---- ModelAllowed：模型白名单 ----

func TestModelAllowed(t *testing.T) {
	cases := []struct {
		name      string
		allowList []string
		model     string
		want      bool
	}{
		{"空白名单允许全部", nil, "any-model", true},
		{"空白名单允许全部(空数组)", []string{}, "gpt-4", true},
		{"命中", []string{"gpt-4", "claude-3"}, "gpt-4", true},
		{"未命中", []string{"gpt-4"}, "gpt-3.5", false},
		{"空模型名未命中", []string{"gpt-4"}, "", false},
		{"大小写敏感", []string{"GPT-4"}, "gpt-4", false},
	}
	for _, c := range cases {
		if got := ModelAllowed(c.allowList, c.model); got != c.want {
			t.Errorf("%s: ModelAllowed(%v, %q) = %v, want %v", c.name, c.allowList, c.model, got, c.want)
		}
	}
}

// Identity 携带白名单字段：鉴权后网关可直接使用（字段存在性/传递性编译期由调用方保证，
// 此处验证零值 Identity 白名单为空 => 全放行，保证升级存量密钥行为不变）。
func TestIdentityDefaultAllowsAll(t *testing.T) {
	ident := &Identity{}
	if !IPAllowed(ident.IPWhitelist, "1.2.3.4") {
		t.Fatal("nil IPWhitelist should allow all IPs")
	}
	if !ModelAllowed(ident.ModelAllowList, "gpt-4") {
		t.Fatal("nil ModelAllowList should allow all models")
	}
}

// toPGTextArray：nil 规范化为空数组（PG 插入空数组而非 NULL）
func TestToPGTextArray(t *testing.T) {
	if got := toPGTextArray(nil); got == nil || len(got) != 0 {
		t.Fatalf("toPGTextArray(nil) = %v, want empty non-nil slice", got)
	}
	in := []string{"a"}
	if got := toPGTextArray(in); len(got) != 1 || got[0] != "a" {
		t.Fatalf("toPGTextArray non-empty mutated: %v", got)
	}
}
