package dbtestify

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	_ "github.com/mattn/go-sqlite3"
)

var ErrInvalidDBDriver = errors.New("invalid db driver")

// DBConnector is an interface for database operations that is used from dbtestify package.
// It absorbs the database driver specific operations like getting table names, primary keys, and performing CRUD operations.
type DBConnector interface {
	TableNames(ctx context.Context, schema ...string) ([]string, error)
	PrimaryKeys(ctx context.Context, table string) ([]string, error)
	Insert(ctx context.Context, tx *sql.Tx, tableName string, columns []string, values []any) error
	Delete(ctx context.Context, tx *sql.Tx, tableName string, columns []string, values []any) error
	Upsert(ctx context.Context, tx *sql.Tx, tableName string, columns, pKeys []string, values []any) error
	Truncate(ctx context.Context, tx *sql.Tx, tableName string) error
	DB() *sql.DB
}

// NewDBConnector creates a new DBConnector based on the provided source string.
// The source string should be in the format of "mysql://", "sqlite://", or "postgres://".
//
// The context is used to manage the lifecycle of the database connection. So you should pass a context that can be cancelled.
//
// This package uses https://github.com/jackc/pgx for PostgreSQL, https://github.com/go-sql-driver/mysql for MySQL, https://github.com/mattn/go-sqlite3 for SQLite3.
// To study the detail of the connection string, check the documentation of each driver.
//
//   - postgres://user:pass@localhost:5432/dbname?sslmode=disable
//   - mysql://root:pass@tcp(localhost:3306)/foo?tls=skip-verify
//   - sqlite://file:dbfilename.db
func NewDBConnector(ctx context.Context, source string) (DBConnector, error) {
	if strings.HasPrefix(source, "mysql://") {
		source = strings.TrimPrefix(source, "mysql://")
		db, err := sql.Open("mysql", source)
		if err != nil {
			return nil, fmt.Errorf("%w: can't connect to MySQL '%s': %s", ErrInvalidDBDriver, err.Error(), source)
		}
		go func() {
			<-ctx.Done()
			db.Close()
		}()
		return &mysqlDBConnector{db: db}, nil
	} else if strings.HasPrefix(source, "sqlite://") || strings.HasPrefix(source, "sqlite3://") {
		source = strings.TrimPrefix(strings.TrimPrefix(source, "sqlite://"), "sqlite3://")
		db, err := sql.Open("sqlite3", source)
		if err != nil {
			return nil, fmt.Errorf("%w: can't open SQLite3 database file '%s': %s", ErrInvalidDBDriver, err.Error(), source)
		}
		go func() {
			<-ctx.Done()
			db.Close()
		}()
		return &sqliteDBConnector{db: db}, nil
	} else if strings.HasPrefix(source, "postgres://") {
		db, err := sql.Open("pgx", source)
		if err != nil {
			return nil, fmt.Errorf("%w: can't connect to PostgreSQL '%s': %s", ErrInvalidDBDriver, err.Error(), source)
		}
		go func() {
			<-ctx.Done()
			db.Close()
		}()
		return &psqlDBConnector{db: db}, nil
	} else {
		return nil, fmt.Errorf("%w: invalid driver '%s'", ErrInvalidDBDriver, source)
	}
}

type psqlDBConnector struct {
	db *sql.DB
}

func (p *psqlDBConnector) TableNames(ctx context.Context, schema ...string) ([]string, error) {
	var s string
	if len(schema) == 0 {
		err := p.db.QueryRowContext(ctx, `SELECT current_schema();`).Scan(&s)
		if err != nil {
			return nil, err
		}
	} else {
		s = schema[0]
	}
	rows, err := p.db.QueryContext(ctx, `
		SELECT
			tablename
		FROM
			pg_catalog.pg_tables
		WHERE
			schemaname = $1
		ORDER BY
			tablename;`, s)
	if err != nil {
		return nil, err
	}
	var result []string
	for rows.Next() {
		var name string
		err := rows.Scan(&name)
		if err != nil {
			return nil, err
		}
		result = append(result, name)
	}
	return result, nil
}

func (p *psqlDBConnector) PrimaryKeys(ctx context.Context, table string) ([]string, error) {
	var schema, tname string
	f := strings.SplitN(table, ".", 2)
	if len(f) == 2 {
		schema = f[0]
		tname = f[1]
	} else {
		err := p.db.QueryRowContext(ctx, `SELECT current_schema();`).Scan(&schema)
		if err != nil {
			return nil, err
		}
		tname = table
	}
	rows, err := p.db.QueryContext(ctx, `
		SELECT
			kcu.column_name
		FROM
			information_schema.table_constraints AS tc
		JOIN
			information_schema.key_column_usage AS kcu
		ON
			tc.constraint_name = kcu.constraint_name
		AND
			tc.table_schema = kcu.table_schema
		WHERE
			tc.constraint_type = 'PRIMARY KEY'
		AND
			tc.table_schema = $1
		AND
			tc.table_name = $2
		ORDER BY
			kcu.column_name;
	`, schema, tname)
	if err != nil {
		return nil, err
	}
	var result []string
	for rows.Next() {
		var name string
		err := rows.Scan(&name)
		if err != nil {
			return nil, err
		}
		result = append(result, name)
	}
	return result, nil
}

func (p *psqlDBConnector) DB() *sql.DB {
	return p.db
}

func pgPlaceholders(columns, rows int) string {
	c := 1
	var placeholders []string
	for r := 0; r < rows; r++ {
		var rp []string
		for i := 0; i < columns; i++ {
			rp = append(rp, "$"+strconv.Itoa(c))
			c++
		}
		placeholders = append(placeholders, "("+strings.Join(rp, ", ")+")")
	}
	return strings.Join(placeholders, ", ")
}

func (p *psqlDBConnector) Insert(ctx context.Context, tx *sql.Tx, tableName string, columns []string, values []any) error {
	insertStmt := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES %s",
		tableName,
		strings.Join(columns, ", "),
		pgPlaceholders(len(columns), len(values)/len(columns)),
	)
	_, err := tx.ExecContext(ctx, insertStmt, values...)
	return err
}

func (p *psqlDBConnector) Delete(ctx context.Context, tx *sql.Tx, tableName string, columns []string, values []any) error {
	var columnStr string
	var placeholderStr string
	if len(columns) > 1 {
		columnStr = "(" + strings.Join(columns, ", ") + ")"
		placeholderStr = pgPlaceholders(len(columns), len(values)/len(columns))
	} else {
		columnStr = columns[0]
		var placeholders []string
		for i := range len(values) {
			placeholders = append(placeholders, "$"+strconv.Itoa(i+1))
		}
		placeholderStr = strings.Join(placeholders, ", ")
	}

	deleteStmt := fmt.Sprintf(
		"DELETE FROM %s WHERE %s IN (%s)",
		tableName,
		columnStr,
		placeholderStr,
	)
	_, err := tx.ExecContext(ctx, deleteStmt, values...)
	return err
}

// Truncate implements DBConnector.
func (p *psqlDBConnector) Truncate(ctx context.Context, tx *sql.Tx, tableName string) error {
	_, err := tx.ExecContext(ctx, fmt.Sprintf("TRUNCATE TABLE %s;", tableName))
	return err
}

// Upsert implements DBConnector.
func (p *psqlDBConnector) Upsert(ctx context.Context, tx *sql.Tx, tableName string, columns, pKeys []string, values []any) error {
	var assigns []string
	for _, column := range columns {
		if !slices.Contains(pKeys, column) {
			assigns = append(assigns, column+" = excluded."+column)
		}
	}

	insertStmt := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES %s ON CONFLICT(%s) DO UPDATE SET %s;",
		tableName,
		strings.Join(columns, ", "),
		pgPlaceholders(len(columns), len(values)/len(columns)),
		strings.Join(pKeys, ", "),
		strings.Join(assigns, ", "),
	)
	_, err := tx.ExecContext(ctx, insertStmt, values...)
	return err
}

var _ DBConnector = (*psqlDBConnector)(nil)

type mysqlDBConnector struct {
	db *sql.DB
}

func (m *mysqlDBConnector) TableNames(ctx context.Context, schema ...string) ([]string, error) {
	var s string
	if len(schema) == 0 {
		err := m.db.QueryRowContext(ctx, `SELECT DATABASE();`).Scan(&s)
		if err != nil {
			return nil, err
		}
	} else {
		s = schema[0]
	}
	rows, err := m.db.QueryContext(ctx, `
		SELECT
			t.table_name
		FROM
			information_schema.tables AS t
		WHERE
			t.table_schema = ?
		AND
			t.table_type = 'BASE TABLE'
		ORDER BY
			t.table_name;
	`, "foo")
	if err != nil {
		return nil, err
	}
	var result []string
	for rows.Next() {
		var name string
		err := rows.Scan(&name)
		if err != nil {
			return nil, err
		}
		result = append(result, name)
	}
	return result, nil
}

func (m *mysqlDBConnector) PrimaryKeys(ctx context.Context, table string) ([]string, error) {
	var schema, tname string
	f := strings.SplitN(table, ".", 2)
	if len(f) == 2 {
		schema = f[0]
		tname = f[1]
	} else {
		err := m.db.QueryRowContext(ctx, `SELECT DATABASE();`).Scan(&schema)
		if err != nil {
			return nil, err
		}
		tname = table
	}
	rows, err := m.db.QueryContext(ctx, `
		SELECT
			c.COLUMN_NAME
		FROM
			information_schema.COLUMNS AS c
		WHERE
			c.TABLE_SCHEMA = ?
		AND
			c.TABLE_NAME = ?
		AND
			c.COLUMN_KEY = 'PRI'
		ORDER BY
			c.COLUMN_NAME;
	`, schema, tname)
	if err != nil {
		return nil, err
	}
	var result []string
	for rows.Next() {
		var name string
		err := rows.Scan(&name)
		if err != nil {
			return nil, err
		}
		result = append(result, name)
	}
	return result, nil
}

func (p *mysqlDBConnector) DB() *sql.DB {
	return p.db
}

func (m *mysqlDBConnector) Insert(ctx context.Context, tx *sql.Tx, tableName string, columns []string, values []any) error {
	insertStmt := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES %s;",
		tableName,
		strings.Join(columns, ", "),
		slPlaceholders(len(columns), len(values)/len(columns)),
	)
	_, err := tx.ExecContext(ctx, insertStmt, values...)
	return err
}

func (m *mysqlDBConnector) Delete(ctx context.Context, tx *sql.Tx, tableName string, columns []string, values []any) error {
	var columnStr string
	var placeholderStr string
	if len(columns) > 1 {
		columnStr = "(" + strings.Join(columns, ", ") + ")"
		placeholderStr = slPlaceholders(len(columns), len(values)/len(columns))
	} else {
		columnStr = columns[0]
		placeholderStr = strings.Join(slices.Repeat([]string{"?"}, len(values)), ", ")
	}

	deleteStmt := fmt.Sprintf(
		"DELETE FROM %s WHERE %s IN (%s);",
		tableName,
		columnStr,
		placeholderStr,
	)

	_, err := tx.ExecContext(ctx, deleteStmt, values...)
	return err
}

func (m *mysqlDBConnector) Upsert(ctx context.Context, tx *sql.Tx, tableName string, columns, pKeys []string, values []any) error {
	var assigns []string
	for _, column := range columns {
		if !slices.Contains(pKeys, column) {
			assigns = append(assigns, column+" = VALUES("+column+")")
		}
	}

	insertStmt := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES %s ON DUPLICATE KEY UPDATE %s;",
		tableName,
		strings.Join(columns, ", "),
		slPlaceholders(len(columns), len(values)/len(columns)),
		strings.Join(assigns, ", "),
	)
	_, err := tx.ExecContext(ctx, insertStmt, values...)
	return err
}

func (m *mysqlDBConnector) Truncate(ctx context.Context, tx *sql.Tx, tableName string) error {
	_, err := tx.ExecContext(ctx, fmt.Sprintf("TRUNCATE TABLE %s;", tableName))
	return err
}

var _ DBConnector = (*mysqlDBConnector)(nil)

type sqliteDBConnector struct {
	db *sql.DB
}

func (s *sqliteDBConnector) TableNames(ctx context.Context, schema ...string) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT
			sm.name
		FROM
			sqlite_master AS sm
		WHERE
			sm.type='table'
		ORDER BY
			sm.name;`)
	if err != nil {
		return nil, err
	}
	var result []string
	for rows.Next() {
		var name string
		err := rows.Scan(&name)
		if err != nil {
			return nil, err
		}
		result = append(result, name)
	}
	return result, nil
}

func (s *sqliteDBConnector) PrimaryKeys(ctx context.Context, table string) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT
			ti.name
		FROM
			pragma_table_info(?) AS ti
		WHERE
			ti.pk <> 0
		ORDER BY
			ti.name;`, table)
	if err != nil {
		return nil, err
	}
	var result []string
	for rows.Next() {
		var name string
		err := rows.Scan(&name)
		if err != nil {
			return nil, err
		}
		result = append(result, name)
	}
	return result, nil
}

func (p *sqliteDBConnector) DB() *sql.DB {
	return p.db
}

func slPlaceholders(columns, records int) string {
	rp := "(" + strings.Join(slices.Repeat([]string{"?"}, columns), ", ") + ")"
	placeholders := slices.Repeat([]string{rp}, records)
	return strings.Join(placeholders, ", ")
}

func (s *sqliteDBConnector) Insert(ctx context.Context, tx *sql.Tx, tableName string, columns []string, values []any) error {
	insertStmt := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES %s",
		tableName,
		strings.Join(columns, ", "),
		slPlaceholders(len(columns), len(values)/len(columns)),
	)
	_, err := tx.ExecContext(ctx, insertStmt, values...)
	return err
}

func (s *sqliteDBConnector) Delete(ctx context.Context, tx *sql.Tx, tableName string, columns []string, values []any) error {
	var columnStr string
	var placeholderStr string
	if len(columns) > 1 {
		columnStr = "(" + strings.Join(columns, ", ") + ")"
		placeholderStr = slPlaceholders(len(columns), len(values)/len(columns))
	} else {
		columnStr = columns[0]
		placeholderStr = strings.Join(slices.Repeat([]string{"?"}, len(values)), ", ")
	}

	deleteStmt := fmt.Sprintf(
		"DELETE FROM %s WHERE %s IN (%s)",
		tableName,
		columnStr,
		placeholderStr,
	)
	_, err := tx.ExecContext(ctx, deleteStmt, values...)
	return err
}

func (s *sqliteDBConnector) Upsert(ctx context.Context, tx *sql.Tx, tableName string, columns, pKeys []string, values []any) error {
	var assigns []string
	for _, column := range columns {
		if !slices.Contains(pKeys, column) {
			assigns = append(assigns, column+" = excluded."+column)
		}
	}

	insertStmt := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES %s ON CONFLICT(%s) DO UPDATE SET %s;",
		tableName,
		strings.Join(columns, ", "),
		slPlaceholders(len(columns), len(values)/len(columns)),
		strings.Join(pKeys, ", "),
		strings.Join(assigns, ", "),
	)
	_, err := tx.ExecContext(ctx, insertStmt, values...)
	return err
}

func (s *sqliteDBConnector) Truncate(ctx context.Context, tx *sql.Tx, tableName string) error {
	truncateSrc := fmt.Sprintf("DELETE FROM %s;", tableName)
	_, err := tx.ExecContext(ctx, truncateSrc)
	return err
}

var _ DBConnector = (*sqliteDBConnector)(nil)
