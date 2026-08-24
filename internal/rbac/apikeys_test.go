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
