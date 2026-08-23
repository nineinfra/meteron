// Package store 封装 PG / ClickHouse / Redis / Kafka 连接与初始化。
package store

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

// NewPG 创建 PG 连接池并对齐必要参数。
func NewPG(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse pg dsn: %w", err)
	}
	cfg.MaxConns = 20
	cfg.MinConns = 2
	cfg.MaxConnLifetime = time.Hour
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect pg: %w", err)
	}
	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		return nil, fmt.Errorf("ping pg: %w", err)
	}
	return pool, nil
}

// MigratePG 执行内嵌迁移脚本（幂等：脚本内部使用 IF NOT EXISTS / CREATE TYPE 前置删除兼容）。
func MigratePG(ctx context.Context, pool *pgxpool.Pool) error {
	if _, err := pool.Exec(ctx, `
CREATE TABLE IF NOT EXISTS schema_migrations (
    version TEXT PRIMARY KEY,
    applied_at TIMESTAMP NOT NULL DEFAULT NOW()
)`); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}

	entries, err := fs.ReadDir(migrationFS, "migrations")
	if err != nil {
		return err
	}
	var names []string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".sql") && strings.HasPrefix(e.Name(), "001") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)

	for _, name := range names {
		var exists bool
		if err := pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version=$1)`, name).Scan(&exists); err != nil {
			return err
		}
		if exists {
			continue
		}
		data, err := migrationFS.ReadFile("migrations/" + name)
		if err != nil {
			return err
		}
		// 001 脚本可能重复执行（幂等保护）：逐条执行，忽略已存在对象错误
		stmts := splitSQL(string(data))
		tx, err := pool.Begin(ctx)
		if err != nil {
			return err
		}
		for _, s := range stmts {
			if strings.TrimSpace(s) == "" {
				continue
			}
			// CREATE TYPE 幂等：删除已存在类型再创建
			s = idempotentDDL(s)
			if _, err := tx.Exec(ctx, s); err != nil {
				if isAlreadyExists(err) {
					continue
				}
				_ = tx.Rollback(ctx)
				return fmt.Errorf("migrate %s stmt %.80s: %w", name, s, err)
			}
		}
		if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations(version) VALUES($1)`, name); err != nil {
			_ = tx.Rollback(ctx)
			return err
		}
		if err := tx.Commit(ctx); err != nil {
			return err
		}
	}
	return nil
}

// splitSQL 粗粒度按分号拆分（脚本内无函数体/存储过程，安全）。
func splitSQL(s string) []string {
	return strings.Split(s, ";")
}

// idempotentDDL 将 CREATE TYPE 转换为幂等形式。
func idempotentDDL(s string) string {
	t := strings.TrimSpace(s)
	up := strings.ToUpper(t)
	if strings.HasPrefix(up, "CREATE TYPE ") {
		// CREATE TYPE xxx AS ENUM (...) -> DO $$ BEGIN ... EXCEPTION ... END $$;
		return "DO $$ BEGIN " + t + "; EXCEPTION WHEN duplicate_object THEN NULL; END $$;"
	}
	return t
}

func isAlreadyExists(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "already exists")
}
