package store

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// chDDL 读取内嵌的 ClickHouse 建表脚本（来自 migrationFS）。
func chDDL() (string, error) {
	b, err := migrationFS.ReadFile("migrations/002_clickhouse_init.sql")
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// CHOptions 从 DSN（clickhouse://user:pass@host:port/db）解析 ClickHouse 连接参数。
func CHOptions(dsn string) (*clickhouse.Options, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse clickhouse dsn: %w", err)
	}
	host := u.Hostname()
	port := u.Port()
	if port == "" {
		port = "9000"
	}
	user := "default"
	pass := ""
	if u.User != nil {
		user = u.User.Username()
		pass, _ = u.User.Password()
	}
	db := strings.TrimPrefix(u.Path, "/")
	if db == "" {
		db = "default"
	}
	return &clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%s", host, port)},
		Auth: clickhouse.Auth{
			Database: db,
			Username: user,
			Password: pass,
		},
		DialTimeout:     10 * time.Second,
		MaxOpenConns:    10,
		MaxIdleConns:    4,
		ConnMaxLifetime: time.Hour,
	}, nil
}

// NewClickHouse 建立 ClickHouse 连接并执行 DDL（幂等）。
func NewClickHouse(ctx context.Context, dsn string) (driver.Conn, error) {
	opts, err := CHOptions(dsn)
	if err != nil {
		return nil, err
	}
	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("open clickhouse: %w", err)
	}
	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := conn.Ping(pingCtx); err != nil {
		return nil, fmt.Errorf("ping clickhouse: %w", err)
	}
	ddl, err := chDDL()
	if err != nil {
		return nil, err
	}
	// clickhouse-go 不支持 multi-statement：按分号拆分逐条执行（幂等 CREATE IF NOT EXISTS）
	for _, stmt := range strings.Split(ddl, ";") {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		if err := conn.Exec(ctx, stmt); err != nil {
			return nil, fmt.Errorf("apply clickhouse ddl: %w", err)
		}
	}
	return conn, nil
}
