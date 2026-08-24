package store

import (
	"io/fs"
	"strings"
	"testing"
)

// 静态文件名列表驱动：验证 PG 迁移筛选规则（含 "_pg"）与排序。
type fakeDirEntry struct{ name string }

func (f fakeDirEntry) Name() string                               { return f.name }
func (f fakeDirEntry) IsDir() bool                                { return false }
func (f fakeDirEntry) Type() fs.FileMode                          { return 0 }
func (f fakeDirEntry) Info() (fs.FileInfo, error)                 { return nil, fs.ErrNotExist }

// P1 修复点：新增 PG 迁移（如 003_meter_outbox_pg.sql）必须被选中，
// ClickHouse 脚本必须被排除（由 NewClickHouse 单独应用）。
func TestPGMigrationNames(t *testing.T) {
	entries := []fs.DirEntry{
		fakeDirEntry{"002_clickhouse_init.sql"},
		fakeDirEntry{"003_meter_outbox_pg.sql"},
		fakeDirEntry{"001_pg_init.sql"},
		fakeDirEntry{"notes.txt"},           // 非 SQL 忽略
		fakeDirEntry{"004_clickhouse_pg.sql"}, // 含 _pg 视为 PG 迁移（命名约定）
	}
	got := pgMigrationNames(entries)
	want := []string{"001_pg_init.sql", "003_meter_outbox_pg.sql", "004_clickhouse_pg.sql"}
	if len(got) != len(want) {
		t.Fatalf("pgMigrationNames = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("pgMigrationNames[%d] = %s, want %s (must be sorted)", i, got[i], want[i])
		}
	}
}

func TestSplitSQL(t *testing.T) {
	stmts := splitSQL("CREATE TABLE a(x INT);\nCREATE TABLE b(y INT);")
	if len(stmts) != 3 { // 尾部空串
		t.Fatalf("splitSQL count = %d, want 3", len(stmts))
	}
	if strings.TrimSpace(stmts[0]) != "CREATE TABLE a(x INT)" {
		t.Fatalf("stmt[0] = %q", stmts[0])
	}
}

// CREATE TYPE 需包装为幂等 DO 块（重复执行不报 duplicate_object），
// 其余语句原样返回。
func TestIdempotentDDL(t *testing.T) {
	ty := idempotentDDL("CREATE TYPE foo AS ENUM ('a')")
	if !strings.HasPrefix(ty, "DO $$ BEGIN CREATE TYPE") || !strings.Contains(ty, "EXCEPTION WHEN duplicate_object") {
		t.Fatalf("CREATE TYPE not wrapped: %q", ty)
	}
	tbl := idempotentDDL("CREATE TABLE IF NOT EXISTS t(id INT)")
	if tbl != "CREATE TABLE IF NOT EXISTS t(id INT)" {
		t.Fatalf("other DDL must be unchanged: %q", tbl)
	}
}
