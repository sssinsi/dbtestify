package dbtestify

import (
	"context"
	"database/sql"
	"log"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mysql"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func TestDBConnectorPostgreSQL(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	ctx := context.Background()

	pgContainer, err := postgres.Run(ctx, "postgres:15.3-alpine",
		postgres.WithInitScripts(filepath.Join("testdata", "db-connector-test-init.sql")),
		postgres.WithDatabase("dbconnectortest"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(5*time.Second)),
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := pgContainer.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate pgContainer: %s", err)
		}
	})
	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	assert.NoError(t, err)

	ctx2, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, err := NewDBConnector(ctx2, connStr)
	assert.NoError(t, err)

	// with schema
	tnames, err := db.TableNames(ctx2, "public")
	assert.NoError(t, err)
	assert.Equal(t, []string{"book_authors", "orders", "student_course_enrollments"}, tnames)

	// without schema
	tnames, err = db.TableNames(ctx2)
	assert.NoError(t, err)
	assert.Equal(t, []string{"book_authors", "orders", "student_course_enrollments"}, tnames)

	// with schema
	pkeys, err := db.PrimaryKeys(ctx2, "public.orders")
	assert.NoError(t, err)
	assert.Equal(t, []string{"order_id", "product_id"}, pkeys)

	// without schema
	pkeys, err = db.PrimaryKeys(ctx2, "orders")
	assert.NoError(t, err)
	assert.Equal(t, []string{"order_id", "product_id"}, pkeys)
}

func TestDBConnectionMySQL(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	ctx := context.Background()

	mysqlContainer, err := mysql.Run(ctx, "mysql:8",
		mysql.WithDatabase("foo"),
		mysql.WithUsername("root"),
		mysql.WithPassword("password"),
		mysql.WithScripts(filepath.Join("testdata", "db-connector-test-init.sql")),
	)
	assert.NoError(t, err)
	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(mysqlContainer); err != nil {
			t.Fatalf("failed to terminate mysqlContainer: %s", err)
		}
	})
	connStr, err := mysqlContainer.ConnectionString(ctx, "tls=skip-verify")
	assert.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, err := NewDBConnector(ctx, "mysql://"+connStr)
	assert.NoError(t, err)

	// with schema
	tnames, err := db.TableNames(ctx, "foo")
	assert.NoError(t, err)
	assert.Equal(t, []string{"book_authors", "orders", "student_course_enrollments"}, tnames)

	// without schema
	tnames, err = db.TableNames(ctx)
	assert.NoError(t, err)
	assert.Equal(t, []string{"book_authors", "orders", "student_course_enrollments"}, tnames)

	// with schema
	pkeys, err := db.PrimaryKeys(ctx, "foo.orders")
	assert.NoError(t, err)
	assert.Equal(t, []string{"order_id", "product_id"}, pkeys)

	// without schema
	pkeys, err = db.PrimaryKeys(ctx, "orders")
	assert.NoError(t, err)
	assert.Equal(t, []string{"order_id", "product_id"}, pkeys)
}

func TestDBConnectionSQLite(t *testing.T) {
	os.Remove("get_db_status.db")
	connStr := "file:get_db_status.db?cache=shared&mode=rwc"
	err := func() error {
		db, err := sql.Open("sqlite3", connStr)
		if err != nil {
			return err
		}
		defer db.Close()
		initSql, err := os.ReadFile(filepath.Join("testdata", "db-connector-test-init.sql"))
		if err != nil {
			return err
		}
		_, err = db.Exec(string(initSql))
		return err
	}()

	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, err := NewDBConnector(ctx, "sqlite://"+connStr)
	assert.NoError(t, err)

	tnames, err := db.TableNames(ctx)
	assert.NoError(t, err)
	assert.Equal(t, []string{"book_authors", "orders", "student_course_enrollments"}, tnames)

	pkeys, err := db.PrimaryKeys(ctx, "orders")
	assert.NoError(t, err)
	assert.Equal(t, []string{"order_id", "product_id"}, pkeys)
}

func TestSQLiteDeleteCompositeKey(t *testing.T) {
	os.Remove("test_delete_composite.db")
	connStr := "file:test_delete_composite.db?cache=shared&mode=rwc"
	err := func() error {
		db, err := sql.Open("sqlite3", connStr)
		if err != nil {
			return err
		}
		defer db.Close()
		initSql, err := os.ReadFile(filepath.Join("testdata", "db-connector-test-init.sql"))
		if err != nil {
			return err
		}
		_, err = db.Exec(string(initSql))
		return err
	}()

	if err != nil {
		t.Fatal(err)
	}
	
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, err := NewDBConnector(ctx, "sqlite://"+connStr)
	assert.NoError(t, err)

	// テストデータを挿入
	tx, err := db.DB().Begin()
	assert.NoError(t, err)
	defer tx.Rollback()

	// Insert test data
	err = db.Insert(ctx, tx, "orders", []string{"order_id", "product_id", "quantity", "price"}, 
		[]any{1, 100, 5, 1000, 2, 101, 3, 500})
	assert.NoError(t, err)

	// 複合キーでのDelete操作をテスト
	err = db.Delete(ctx, tx, "orders", []string{"order_id", "product_id"}, []any{1, 100})
	assert.NoError(t, err, "複合キーでのDelete操作が失敗しました")

	// データが削除されたか確認
	var count int
	err = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM orders WHERE order_id = 1 AND product_id = 100").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 0, count, "データが削除されていません")

	// 他のデータが残っているか確認
	err = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM orders WHERE order_id = 2 AND product_id = 101").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 1, count, "他のデータが誤って削除されました")

	tx.Commit()
}

func TestMySQLDeleteCompositeKey(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	ctx := context.Background()

	mysqlContainer, err := mysql.Run(ctx, "mysql:8",
		mysql.WithDatabase("test_delete"),
		mysql.WithUsername("root"),
		mysql.WithPassword("password"),
		mysql.WithScripts(filepath.Join("testdata", "db-connector-test-init.sql")),
	)
	assert.NoError(t, err)
	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(mysqlContainer); err != nil {
			t.Fatalf("failed to terminate mysqlContainer: %s", err)
		}
	})
	connStr, err := mysqlContainer.ConnectionString(ctx, "tls=skip-verify")
	assert.NoError(t, err)

	ctx2, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, err := NewDBConnector(ctx2, "mysql://"+connStr)
	assert.NoError(t, err)

	// テストデータを挿入
	tx, err := db.DB().Begin()
	assert.NoError(t, err)
	defer tx.Rollback()

	// Insert test data
	err = db.Insert(ctx2, tx, "orders", []string{"order_id", "product_id", "quantity", "price"}, 
		[]any{1, 100, 5, 1000, 2, 101, 3, 500})
	assert.NoError(t, err)

	// 複合キーでのDelete操作をテスト
	err = db.Delete(ctx2, tx, "orders", []string{"order_id", "product_id"}, []any{1, 100})
	assert.NoError(t, err, "複合キーでのDelete操作が失敗しました")

	// データが削除されたか確認
	var count int
	err = tx.QueryRowContext(ctx2, "SELECT COUNT(*) FROM orders WHERE order_id = 1 AND product_id = 100").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 0, count, "データが削除されていません")

	// 他のデータが残っているか確認
	err = tx.QueryRowContext(ctx2, "SELECT COUNT(*) FROM orders WHERE order_id = 2 AND product_id = 101").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 1, count, "他のデータが誤って削除されました")

	tx.Commit()
}
