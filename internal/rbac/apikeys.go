// Package rbac 提供 API Key 的确定性 AES 加密存储与鉴权。
// key_hash 列保存 AES-GCM 确定性加密结果（同一明文+主密钥 => 同一密文），
// 从而支持"加密后等值查找"，全程不落明文。
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
	db      *pgxpool.Pool
	aesGCM  cipher.AEAD
	hmacKey []byte
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
	return &Service{db: db, aesGCM: gcm, hmacKey: key}, nil
}

// encrypt 确定性加密：nonce = HMAC(key, plaintext)[:12]，同一明文密文稳定可查。
func (s *Service) encrypt(plaintext string) string {
	mac := hmac.New(sha256.New, s.hmacKey)
	mac.Write([]byte(plaintext))
	nonce := mac.Sum(nil)[:s.aesGCM.NonceSize()]
	sealed := s.aesGCM.Seal(nil, nonce, []byte(plaintext), nil)
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
		id.String(), tenantID.String(), customerArg, s.encrypt(plaintext), string(scopesJSON), expireArg)
	return id, plaintext, err
}

// Bootstrap 首次启动：确保默认租户与引导密钥存在。
func (s *Service) Bootstrap(ctx context.Context, bootstrapKey string) (tenantID uuid.UUID, created bool, err error) {
	var tid string
	err = s.db.QueryRow(ctx, `SELECT id::text FROM tenants ORDER BY created_at LIMIT 1`).Scan(&tid)
	if err == nil {
		tenantID, _ = uuid.Parse(tid)
		// 确保引导密钥可用（幂等）
		hash := s.encrypt(bootstrapKey)
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
		uuid.NewString(), tenantID.String(), s.encrypt(bootstrapKey), string(scopes)); err != nil {
		return uuid.Nil, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return uuid.Nil, false, err
	}
	return tenantID, true, nil
}

// Authenticate 校验 Bearer 明文密钥并返回身份。
func (s *Service) Authenticate(ctx context.Context, plaintext string) (*Identity, error) {
	hash := s.encrypt(plaintext)
	var (
		idStr, tenantStr     string
		customerStr          *string
		scopeJSON            []byte
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
