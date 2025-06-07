package gounittest

import (
	"embed"
	"os"
	"testing"

	"github.com/shibukawa/dbtestify/assertdb"
)

//go:embed dataset/*
var dataSet embed.FS

func TestMain(m *testing.M) {
	// Initialize the database before running tests
	db, err := InitDB()
	if err != nil {
		os.Stderr.WriteString("Failed to initialize database: " + err.Error() + "\n")
		os.Exit(1)
	}
	db.Close()

	code := m.Run()

	os.Exit(code)
}

func TestCounter(t *testing.T) {
	assertdb.SeedDataSet(t, "sqlite://file:counter.db", dataSet, "dataset/initial.yaml", nil)

	db, err := InitDB()
	if err != nil {
		t.Fatalf("Failed to initialize database: %v", err)
	}
	defer db.Close()

	IncrementCounter(db)

	assertdb.AssertDB(t, "sqlite://file:counter.db", dataSet, "dataset/expect.yaml", nil)
}
