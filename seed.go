package dbtestify

import (
	"context"
	"database/sql"
	"maps"
	"slices"
)

// DefaultBatchSize is the default number of rows to process in a single batch during seeding.
var DefaultBatchSize = 50

// SeedOpt defines options for the seeding process.
type SeedOpt struct {
	BatchSize    int                                                   // default: 50
	Operations   map[string]Operation                                  // Operations to apply to each table. If empty, defaults to ClearInsertOperation.
	IncludeTags  []string                                              // Tags to filter rows of dataset.
	ExcludeTags  []string                                              // Tags to filter rows of dataset.
	TargetTables []string                                              // Only specified tables will be processed.
	Callback     func(targetTable, task string, start bool, err error) // Callback function to report progress and errors during the seeding process.
}

// Seed initializes the database with the provided dataset, applying the specified operations.
func Seed(ctx context.Context, dbc DBConnector, data *DataSet, opt SeedOpt) error {
	if opt.BatchSize == 0 {
		opt.BatchSize = DefaultBatchSize
	}
	tx, err := dbc.DB().BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// truncate first
	ops := map[string]Operation{}
	for t, op := range opt.Operations {
		ops[t] = op
	}
	for _, t := range data.Tables {
		if len(opt.TargetTables) > 0 {
			if !slices.Contains(opt.TargetTables, t.Name) {
				continue
			}
		}
		switch opt.Operations[t.Name] {
		case ClearInsertOperation:
			fallthrough
		case "":
			fallthrough
		case TruncateOperation:
			ops[t.Name] = TruncateOperation
		}
	}
	for t, op := range ops {
		if op == TruncateOperation {
			if opt.Callback != nil {
				opt.Callback(t, "truncate", true, nil)
			}
			err := dbc.Truncate(ctx, tx, t)
			if opt.Callback != nil {
				opt.Callback(t, "truncate", false, err)
			}
			if err != nil {
				return err
			}
		}
	}
	for _, t := range data.Tables {
		if len(opt.TargetTables) > 0 {
			if !slices.Contains(opt.TargetTables, t.Name) {
				continue
			}
		}
		switch opt.Operations[t.Name] {
		case ClearInsertOperation:
			fallthrough
		case "":
			fallthrough
		case InsertOperation:
			if opt.Callback != nil {
				opt.Callback(t.Name, "insert", true, nil)
			}
			err := processInsertOperation(ctx, dbc, tx, t, opt, false)
			if opt.Callback != nil {
				opt.Callback(t.Name, "insert", false, nil)
			}
			if err != nil {
				return err
			}
		case UpsertOperation:
			if opt.Callback != nil {
				opt.Callback(t.Name, "upsert", true, nil)
			}
			err := processInsertOperation(ctx, dbc, tx, t, opt, true)
			if opt.Callback != nil {
				opt.Callback(t.Name, "upsert", false, nil)
			}
			if err != nil {
				return err
			}
		case DeleteOperation:
			if opt.Callback != nil {
				opt.Callback(t.Name, "delete", true, nil)
			}
			err := processDeleteOperation(ctx, dbc, tx, t, opt)
			if opt.Callback != nil {
				opt.Callback(t.Name, "delete", false, err)
			}
			if err != nil {
				return err
			}
		}
	}
	tx.Commit()
	return nil
}

func processInsertOperation(ctx context.Context, dbc DBConnector, tx *sql.Tx, t *Table, opt SeedOpt, upsert bool) error {
	var pKeys []string
	if upsert {
		var err error
		pKeys, err = dbc.PrimaryKeys(ctx, t.Name)
		if err != nil {
			return err
		}

	}

	for i := 0; i < len(t.Rows); i += opt.BatchSize {
		end := i + opt.BatchSize
		if end > len(t.Rows) {
			end = len(t.Rows)
		}
		batch := t.Rows[i:end]
		columnMaps := map[string]bool{}
		for _, r := range batch {
			for k := range maps.Keys(r) {
				columnMaps[k] = true
			}
		}
		columns := slices.Sorted(maps.Keys(columnMaps))
		values := make([]any, 0, len(batch)*len(columns))
		for j, r := range batch {
			if filter(t.Tags[i+j], opt.IncludeTags, opt.ExcludeTags) {
				for _, c := range columns {
					if val, ok := r[c]; ok {
						values = append(values, val)
					} else {
						values = append(values, nil)
					}
				}
			}
		}
		if upsert {
			if err := dbc.Upsert(ctx, tx, t.Name, columns, pKeys, values); err != nil {
				return err
			}
		} else if err := dbc.Insert(ctx, tx, t.Name, columns, values); err != nil {
			return err
		}
	}
	return nil
}

func processDeleteOperation(ctx context.Context, dbc DBConnector, tx *sql.Tx, t *Table, opt SeedOpt) error {
	columns, err := dbc.PrimaryKeys(ctx, t.Name)
	if err != nil {
		return err
	}
	for i := 0; i < len(t.Rows); i += opt.BatchSize {
		end := i + opt.BatchSize
		if end > len(t.Rows) {
			end = len(t.Rows)
		}
		batch := t.Rows[i:end]
		values := make([]any, 0, len(batch)*len(columns))
		for j, r := range batch {
			if filter(t.Tags[i+j], opt.IncludeTags, opt.ExcludeTags) {
				for _, c := range columns {
					if val, ok := r[c]; ok {
						values = append(values, val)
					} else {
						values = append(values, nil)
					}
				}
			}
		}
		if err := dbc.Delete(ctx, tx, t.Name, columns, values); err != nil {
			return err
		}
	}
	return nil
}
