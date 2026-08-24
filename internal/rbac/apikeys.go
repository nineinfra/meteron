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
	"net"
	"strings"
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
	TenantID       uuid.UUID
	CustomerID     uuid.UUID // 可为 nil（平台级密钥）
	KeyID          uuid.UUID
	Scopes         []string
	IPWhitelist    []string // IP 白名单（空=不限制；元素支持单 IP 与 CIDR）
	ModelAllowList []string // 模型白名单（空=允许全部模型）
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

// KeyOptions 创建/更新 API Key 的可选项。
type KeyOptions struct {
	CustomerID     *uuid.UUID // 绑定客户（nil=平台级密钥，仅 tenant 维度）
	Scopes         []string   // 权限范围，默认 gateway_invoke
	ExpireAt       *time.Time // 过期时间（nil=永不过期，过期自动失效）
	IPWhitelist    []string   // IP 白名单（空=不限制）
	ModelAllowList []string   // 模型白名单（空=允许全部）
}

// CreateKey 创建 API Key，返回 id 与明文（仅此一次返回）。
func (s *Service) CreateKey(ctx context.Context, tenantID uuid.UUID, opts KeyOptions) (id uuid.UUID, plaintext string, err error) {
	plaintext = "mk_" + uuid.NewString() + "_" + hex.EncodeToString(randomBytes(8))
	id = uuid.New()
	scopes := opts.Scopes
	if scopes == nil {
		scopes = []string{ScopeGateway}
	}
	scopesJSON, _ := json.Marshal(scopes)
	var customerArg interface{}
	if opts.CustomerID != nil {
		customerArg = opts.CustomerID.String()
	}
	var expireArg interface{}
	if opts.ExpireAt != nil {
		expireArg = *opts.ExpireAt
	}
	_, err = s.db.Exec(ctx, `
INSERT INTO api_keys (id, tenant_id, customer_id, key_hash, scope, expire_at, ip_whitelist, model_allow_list)
VALUES ($1::uuid, $2::uuid, $3::uuid, $4, $5::jsonb, $6, $7, $8)`,
		id.String(), tenantID.String(), customerArg, hashKey(s.hmacKey, plaintext), string(scopesJSON), expireArg,
		toPGTextArray(opts.IPWhitelist), toPGTextArray(opts.ModelAllowList))
	return id, plaintext, err
}

// KeyView API Key 列表/详情视图（不含哈希）。
type KeyView struct {
	ID             uuid.UUID  `json:"id"`
	CustomerID     *uuid.UUID `json:"customer_id"`
	Scopes         []string   `json:"scopes"`
	ExpireAt       *time.Time `json:"expire_at"`
	IPWhitelist    []string   `json:"ip_whitelist"`
	ModelAllowList []string   `json:"model_allow_list"`
	IsEnabled      bool       `json:"is_enabled"`
	CreatedAt      time.Time  `json:"created_at"`
}

// ListKeys 列出租户下未删除的 API Key（不返回哈希）。
func (s *Service) ListKeys(ctx context.Context, tenantID uuid.UUID) ([]KeyView, error) {
	rows, err := s.db.Query(ctx, `
SELECT id::text, customer_id::text, scope, expire_at, ip_whitelist, model_allow_list, is_enabled, created_at
FROM api_keys WHERE tenant_id=$1::uuid AND deleted_at IS NULL
ORDER BY created_at DESC LIMIT 200`, tenantID.String())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []KeyView{}
	for rows.Next() {
		var (
			v              KeyView
			idStr          string
			customerStr    *string
			scopeJSON      []byte
			ipList         []string
			modelList      []string
		)
		if err := rows.Scan(&idStr, &customerStr, &scopeJSON, &v.ExpireAt, &ipList, &modelList, &v.IsEnabled, &v.CreatedAt); err != nil {
			return nil, err
		}
		if v.ID, err = uuid.Parse(idStr); err != nil {
			return nil, err
		}
		if customerStr != nil && *customerStr != "" {
			cid, err := uuid.Parse(*customerStr)
			if err != nil {
				return nil, err
			}
			v.CustomerID = &cid
		}
		if err := json.Unmarshal(scopeJSON, &v.Scopes); err != nil {
			return nil, err
		}
		v.IPWhitelist = ipList
		v.ModelAllowList = modelList
		out = append(out, v)
	}
	return out, rows.Err()
}

// SetKeyEnabled 启用/禁用密钥（软失效，鉴权路径即时生效）。
func (s *Service) SetKeyEnabled(ctx context.Context, tenantID, keyID uuid.UUID, enabled bool) error {
	tag, err := s.db.Exec(ctx, `
UPDATE api_keys SET is_enabled=$1, updated_at=NOW()
WHERE id=$2::uuid AND tenant_id=$3::uuid AND deleted_at IS NULL`, enabled, keyID.String(), tenantID.String())
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrKeyNotFound
	}
	return nil
}

// DeleteKey 软删除密钥。
func (s *Service) DeleteKey(ctx context.Context, tenantID, keyID uuid.UUID) error {
	tag, err := s.db.Exec(ctx, `
UPDATE api_keys SET deleted_at=NOW(), updated_at=NOW()
WHERE id=$1::uuid AND tenant_id=$2::uuid AND deleted_at IS NULL`, keyID.String(), tenantID.String())
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrKeyNotFound
	}
	return nil
}

// ErrKeyNotFound 密钥不存在或不属于该租户。
var ErrKeyNotFound = errors.New("api key not found")

// toPGTextArray 空切片规范化为空数组（PG 不接受 nil 数组参数的默认行为依赖驱动版本，显式置空更稳）。
func toPGTextArray(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
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
// 已禁用/已删除/已过期的密钥一律返回错误（SQL 条件过滤，过期自动失效）。
func (s *Service) Authenticate(ctx context.Context, plaintext string) (*Identity, error) {
	hash := hashKey(s.hmacKey, plaintext)
	var (
		idStr, tenantStr string
		customerStr      *string
		scopeJSON        []byte
		ipList           []string
		modelList        []string
	)
	err := s.db.QueryRow(ctx, `
SELECT id::text, tenant_id::text, customer_id::text, scope, ip_whitelist, model_allow_list
FROM api_keys
WHERE key_hash = $1 AND is_enabled = true AND deleted_at IS NULL
  AND (expire_at IS NULL OR expire_at > NOW())`, hash).
		Scan(&idStr, &tenantStr, &customerStr, &scopeJSON, &ipList, &modelList)
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
	ident.IPWhitelist = ipList
	ident.ModelAllowList = modelList
	return ident, nil
}

// IPAllowed 校验来源 IP 是否在密钥白名单内。
// 空白名单不限制；元素支持单 IP（1.2.3.4）与 CIDR（10.0.0.0/8）；
// 非法元素跳过；无法解析的来源 IP（如空值）在有白名单时拒绝（fail-closed）。
func IPAllowed(whitelist []string, remoteIP string) bool {
	if len(whitelist) == 0 {
		return true
	}
	ip := net.ParseIP(strings.TrimSpace(remoteIP))
	if ip == nil {
		return false
	}
	for _, item := range whitelist {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		if !strings.Contains(item, "/") {
			if net.ParseIP(item).Equal(ip) {
				return true
			}
			continue
		}
		_, ipnet, err := net.ParseCIDR(item)
		if err != nil {
			continue
		}
		if ipnet.Contains(ip) {
			return true
		}
	}
	return false
}

// ModelAllowed 校验模型是否在密钥白名单内（空白名单=允许全部）。
func ModelAllowed(allowList []string, model string) bool {
	if len(allowList) == 0 {
		return true
	}
	for _, m := range allowList {
		if m == model {
			return true
		}
	}
	return false
}

func randomBytes(n int) []byte {
	b := make([]byte, n)
	// 忽略错误：crypto/rand 在主流平台不会失败
	_, _ = rand.Read(b)
	return b
}
