package gounittest

import (
	"database/sql"
	"fmt"

	_ "github.com/mattn/go-sqlite3"
)

const (
	dbFileName     = "counter.db"
	counterName    = "main_counter"
	createTableSQL = `
	CREATE TABLE IF NOT EXISTS counters (
		name TEXT PRIMARY KEY,
		value INTEGER NOT NULL DEFAULT 0
	);`
)

func InitDB() (*sql.DB, error) {
	db, err := sql.Open("sqlite3", dbFileName)
	if err != nil {
		return nil, fmt.Errorf("can't open database: %w", err)
	}
	_, err = db.Exec(createTableSQL)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to initialize: %w", err)
	}

	_, err = db.Exec(`
		INSERT OR IGNORE INTO counters (name, value) VALUES (?, 0);`, counterName)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("fail to reset: %w", err)
	}

	return db, nil
}

func GetCounter(db *sql.DB) (int, error) {
	var value int
	row := db.QueryRow("SELECT value FROM counters WHERE name = ?", counterName)
	err := row.Scan(&value)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("fail to get counter: %w", err)
	}
	return value, nil
}

func IncrementCounter(db *sql.DB) (int, error) {
	tx, err := db.Begin()
	if err != nil {
		return 0, fmt.Errorf("fail to start transaction: %w", err)
	}
	defer tx.Rollback()

	var currentValue int
	err = tx.QueryRow("SELECT value FROM counters WHERE name = ?", counterName).Scan(&currentValue)
	if err != nil {
		return 0, fmt.Errorf("fail to get current counter: %w", err)
	}

	newValue := currentValue + 1
	_, err = tx.Exec("UPDATE counters SET value = ? WHERE name = ?", newValue, counterName)
	if err != nil {
		return 0, fmt.Errorf("fail to update counter: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("fail to commit: %w", err)
	}

	return newValue, nil
}
