// assertdb is a helper for testing package. It seeds/asserts the database with the data from the specified YAML file.
//
// It assumes the yaml is in embed.FS
//
//	//go:embed dataset/*
//	var dataSet embed.FS
//
//	func TestUsage(t *testing.T) {
//	    assertdb.SeedDataSet(t, "sqlite://file:database.db", dataSet, "initial.yaml", nil)
//
//	    // some logic that modifies the database
//
//	    assertdb.AssertDB(t, "sqlite://file:database.db", dataSet, "expect.yaml", nil)
//	}
package assertdb

import (
	"context"
	"io/fs"
	"testing"

	"github.com/shibukawa/dbtestify"
)

// SeedDataSet seeds the database with the data from the specified YAML file.
func SeedDataSet(t *testing.T, dbConn string, folder fs.FS, fileName string, opt *dbtestify.SeedOpt) {
	t.Helper()
	file, err := folder.Open(fileName)
	if err != nil {
		t.Fatalf("Failed to open dataset %s: %v", fileName, err)
		return
	}
	defer file.Close()
	data, err := dbtestify.ParseYAML(file)
	if err != nil {
		t.Fatalf("Failed to parse dataset %s: %v", fileName, err)
		return
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	dbc, err := dbtestify.NewDBConnector(ctx, dbConn)
	if err != nil {
		t.Fatalf("Failed to parse dataset %s: %v", fileName, err)
		return
	}
	if opt == nil {
		opt = &dbtestify.SeedOpt{}
	}
	err = dbtestify.Seed(ctx, dbc, data, *opt)
	if err != nil {
		t.Fatalf("Failed to seed dataset %s: %v", fileName, err)
	}
}

// AssertDB asserts the database state against the data from the specified YAML file.
func AssertDB(t *testing.T, dbConn string, folder fs.FS, fileName string, opt *dbtestify.AssertOpt) {
	t.Helper()
	file, err := folder.Open(fileName)
	if err != nil {
		t.Fatalf("Failed to open dataset %s: %v", fileName, err)
		return
	}
	defer file.Close()
	data, err := dbtestify.ParseYAML(file)
	if err != nil {
		t.Fatalf("Failed to parse dataset %s: %v", fileName, err)
		return
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	dbc, err := dbtestify.NewDBConnector(ctx, dbConn)
	if err != nil {
		t.Fatalf("Failed to create DB connector: %v", err)
		return
	}
	if opt == nil {
		opt = &dbtestify.AssertOpt{}
	}
	opt.DiffCallback = dbtestify.DumpDiffCLICallback(true, true)
	ok, _, err := dbtestify.Assert(ctx, dbc, data, *opt)
	if err != nil {
		t.Fatalf("Failed to assert dataset %s: %v", fileName, err)
		return
	}
	if !ok {
		t.Errorf("Assertion failed for dataset %s", fileName)
	}
}
