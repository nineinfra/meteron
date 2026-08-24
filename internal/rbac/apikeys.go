// Package rbac 提供 API Key 的单向哈希存储与鉴权。
// key_hash 列保存 HMAC-SHA256(主密钥, 明文) 的 hex 结果：
// 单向不可逆——即使数据库与主密钥（AES key）同时泄漏，也无法还原密钥明文；
// 同一明文+主密钥 => 同一哈希，支持等值查找，全程不落明文。
package rbac

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const ScopeAdmin = "admin"
const ScopeGateway = "gateway_invoke"
const ScopeIngest = "ingest"

// Identity 鉴权后的调用方身份。
type Identity struct {
	TenantID   uuid.UUID
	CustomerID uuid.UUID // 可为 nil（平台级密钥）
	KeyID      uuid.UUID
	Scopes     []string
}

func (i *Identity) HasScope(s string) bool {
	for _, sc := range i.Scopes {
		if sc == ScopeAdmin || sc == s {
			return true
		}
	}
	return false
}

type Service struct {
	db        *pgxpool.Pool
	hmacKey   []byte
	legacyGCM cipher.AEAD // 仅用于 Bootstrap 升级 v1 确定性加密的存量 key_hash
}

func NewService(db *pgxpool.Pool, aesKeyHex string) (*Service, error) {
	key, err := hex.DecodeString(aesKeyHex)
	if err != nil || len(key) != 32 {
		return nil, fmt.Errorf("invalid aes key hex")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &Service{db: db, hmacKey: key, legacyGCM: gcm}, nil
}

// hashKey API Key 单向哈希：HMAC-SHA256(主密钥, 明文)。
// 主密钥作为 pepper：即使 key_hash 全库泄漏，离线穷举也需同时持有主密钥；
// 而持有主密钥也无法从哈希反推明文（对比 v1 确定性加密：主密钥+密文=明文）。
func hashKey(hmacKey []byte, plaintext string) string {
	mac := hmac.New(sha256.New, hmacKey)
	mac.Write([]byte(plaintext))
	return hex.EncodeToString(mac.Sum(nil))
}

// legacyKeyHash v1 确定性 AES-GCM 加密（nonce = HMAC(明文)），可逆：
// 持有主密钥即可解密还原明文，存在"密钥库泄漏=全部 API Key 泄漏"的风险。
// 仅保留用于 Bootstrap 一次性升级存量记录，鉴权与新建密钥一律使用 hashKey。
func legacyKeyHash(gcm cipher.AEAD, hmacKey []byte, plaintext string) string {
	mac := hmac.New(sha256.New, hmacKey)
	mac.Write([]byte(plaintext))
	nonce := mac.Sum(nil)[:gcm.NonceSize()]
	sealed := gcm.Seal(nil, nonce, []byte(plaintext), nil)
	return hex.EncodeToString(append(nonce, sealed...))
}

// CreateKey 创建 API Key，返回 id 与明文（仅此一次返回）。
func (s *Service) CreateKey(ctx context.Context, tenantID uuid.UUID, customerID *uuid.UUID, scopes []string, expireAt *time.Time) (id uuid.UUID, plaintext string, err error) {
	plaintext = "mk_" + uuid.NewString() + "_" + hex.EncodeToString(randomBytes(8))
	id = uuid.New()
	if scopes == nil {
		scopes = []string{ScopeGateway}
	}
	scopesJSON, _ := json.Marshal(scopes)
	var customerArg interface{}
	if customerID != nil {
		customerArg = customerID.String()
	}
	var expireArg interface{}
	if expireAt != nil {
		expireArg = *expireAt
	}
	_, err = s.db.Exec(ctx, `
INSERT INTO api_keys (id, tenant_id, customer_id, key_hash, scope, expire_at)
VALUES ($1::uuid, $2::uuid, $3::uuid, $4, $5::jsonb, $6)`,
		id.String(), tenantID.String(), customerArg, hashKey(s.hmacKey, plaintext), string(scopesJSON), expireArg)
	return id, plaintext, err
}

// Bootstrap 首次启动：确保默认租户与引导密钥存在。
// 兼容迁移：v1 的 key_hash 为确定性 AES 加密，此处用配置中的引导密钥明文
// 一次性升级为单向哈希（鉴权路径不再接受旧格式）。
func (s *Service) Bootstrap(ctx context.Context, bootstrapKey string) (tenantID uuid.UUID, created bool, err error) {
	hash := hashKey(s.hmacKey, bootstrapKey)
	legacy := legacyKeyHash(s.legacyGCM, s.hmacKey, bootstrapKey)

	var tid string
	err = s.db.QueryRow(ctx, `SELECT id::text FROM tenants ORDER BY created_at LIMIT 1`).Scan(&tid)
	if err == nil {
		tenantID, _ = uuid.Parse(tid)
		// 存量升级：旧格式引导密钥 → 新单向哈希（无匹配行则为 no-op）
		if _, err := s.db.Exec(ctx,
			`UPDATE api_keys SET key_hash=$1, updated_at=NOW() WHERE key_hash=$2`, hash, legacy); err != nil {
			return tenantID, false, err
		}
		// 确保引导密钥可用（幂等）
		var cnt int
		if err := s.db.QueryRow(ctx, `SELECT COUNT(*) FROM api_keys WHERE key_hash=$1`, hash).Scan(&cnt); err != nil {
			return tenantID, false, err
		}
		if cnt == 0 {
			scopes, _ := json.Marshal([]string{ScopeAdmin, ScopeGateway, ScopeIngest})
			if _, err := s.db.Exec(ctx, `
INSERT INTO api_keys (id, tenant_id, key_hash, scope)
VALUES ($1::uuid, $2::uuid, $3, $4::jsonb)`,
				uuid.NewString(), tid, hash, string(scopes)); err != nil {
				return tenantID, false, err
			}
		}
		return tenantID, false, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, false, err
	}

	// 无租户：创建默认租户 + 引导密钥
	tenantID = uuid.New()
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return uuid.Nil, false, err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `INSERT INTO tenants (id, name) VALUES ($1::uuid, 'default')`, tenantID.String()); err != nil {
		return uuid.Nil, false, err
	}
	scopes, _ := json.Marshal([]string{ScopeAdmin, ScopeGateway, ScopeIngest})
	if _, err := tx.Exec(ctx, `
INSERT INTO api_keys (id, tenant_id, key_hash, scope)
VALUES ($1::uuid, $2::uuid, $3, $4::jsonb)`,
		uuid.NewString(), tenantID.String(), hash, string(scopes)); err != nil {
		return uuid.Nil, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return uuid.Nil, false, err
	}
	return tenantID, true, nil
}

// Authenticate 校验 Bearer 明文密钥并返回身份（仅接受单向哈希格式）。
func (s *Service) Authenticate(ctx context.Context, plaintext string) (*Identity, error) {
	hash := hashKey(s.hmacKey, plaintext)
	var (
		idStr, tenantStr string
		customerStr      *string
		scopeJSON        []byte
	)
	err := s.db.QueryRow(ctx, `
SELECT id::text, tenant_id::text, customer_id::text, scope
FROM api_keys
WHERE key_hash = $1 AND is_enabled = true AND deleted_at IS NULL
  AND (expire_at IS NULL OR expire_at > NOW())`, hash).
		Scan(&idStr, &tenantStr, &customerStr, &scopeJSON)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("invalid api key")
		}
		return nil, err
	}
	ident := &Identity{}
	if ident.TenantID, err = uuid.Parse(tenantStr); err != nil {
		return nil, err
	}
	if ident.KeyID, err = uuid.Parse(idStr); err != nil {
		return nil, err
	}
	if customerStr != nil && *customerStr != "" {
		if ident.CustomerID, err = uuid.Parse(*customerStr); err != nil {
			return nil, err
		}
	}
	if err := json.Unmarshal(scopeJSON, &ident.Scopes); err != nil {
		return nil, err
	}
	return ident, nil
}

func randomBytes(n int) []byte {
	b := make([]byte, n)
	// 忽略错误：crypto/rand 在主流平台不会失败
	_, _ = rand.Read(b)
	return b
}
