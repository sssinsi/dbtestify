package dbtestify

import (
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"

	"github.com/goccy/go-yaml"
)

// Operation represents the type of operation to be performed on the database.
type Operation string

const (
	ClearInsertOperation Operation = "clear-insert"
	InsertOperation      Operation = "insert"
	UpsertOperation      Operation = "upsert"
	DeleteOperation      Operation = "delete"
	TruncateOperation    Operation = "truncate"
	InvalidOperator      Operation = "invalid"
)

func (o Operation) String() string {
	return string(o)
}

// MatchStrategy defines how to match rows in the database.
type MatchStrategy string

const (
	ExactMatchStrategy   MatchStrategy = "exact"
	SubMatchStrategy     MatchStrategy = "sub"
	InvalidMatchStrategy MatchStrategy = "invalid"
)

func (s MatchStrategy) String() string {
	return string(s)
}

// DataSet represents a collection of tables and their associated operations and match strategies.
type DataSet struct {
	Operation map[string]Operation
	Match     map[string]MatchStrategy
	Tables    []*Table
}

// Table represents a single table in the dataset, including its name, rows, and tags.
type Table struct {
	Name string
	Rows []map[string]any
	Tags [][]string
}

// ParseYAML reads a YAML formatted dataset from the provided reader and returns a DataSet object.
func ParseYAML(r io.Reader) (*DataSet, error) {
	temp := dataSet{}
	d := yaml.NewDecoder(r, yaml.AllowDuplicateMapKey())
	if err := d.Decode(&temp); err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	return &DataSet{
		Operation: temp.Operation,
		Match:     temp.Match,
		Tables:    temp.Tables,
	}, nil
}

// ErrMissingPrimaryKey is an error type that indicates that some primary keys are missing from a row.
type ErrMissingPrimaryKey struct {
	MissingKeys []string
	Dump        string
}

func (e ErrMissingPrimaryKey) Error() string {
	return fmt.Sprintf("missing primary keys: [%s]", strings.Join(e.MissingKeys, ", "))
}

// Table.SortAndFilter sorts the rows of the table based on the provided primary keys and filters them based on include and exclude tags.
func (t Table) SortAndFilter(primaryKeys, includeTags, excludeTags []string) (*NormalizedTable, error) {
	slices.Sort(primaryKeys)

	var errs []error

	result := &NormalizedTable{
		Name: t.Name,
	}

	result.Rows = make([][]Value, 0, len(t.Rows))
	for i, rawRow := range t.Rows {
		if filter(t.Tags[i], includeTags, excludeTags) {
			row, err := mapToValues(rawRow, primaryKeys)
			if err != nil {
				errs = append(errs, err)
			} else {
				result.Rows = append(result.Rows, row)
			}
		}
	}
	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	sortRow(result.Rows, primaryKeys)
	return result, nil
}

// NormalizedTable represents a normalized version of a table with its name and sorted rows.
type NormalizedTable struct {
	Name string
	Rows [][]Value
}

// Value represents a key-value pair in a row of a table.
type Value struct {
	Key   string
	Value any
}

type dataSet struct {
	Operation map[string]Operation
	Match     map[string]MatchStrategy
	Tables    []*Table
}

func (d *dataSet) UnmarshalYAML(b []byte) error {
	var rawData map[string]any
	if err := yaml.Unmarshal(b, &rawData); err != nil {
		return err
	}

	for key, val := range rawData {
		valueBytes, err := yaml.Marshal(val)
		if err != nil {
			return fmt.Errorf("failed to marshal value for key %s: %w", key, err)
		}

		switch key {
		case "_operation":
			operations := map[string]Operation{}
			if err := yaml.Unmarshal(valueBytes, &operations); err != nil {
				return fmt.Errorf("failed to unmarshal _strategy: %w", err)
			}
			d.Operation = operations
		case "_match":
			matches := map[string]MatchStrategy{}
			if err := yaml.Unmarshal(valueBytes, &matches); err != nil {
				return fmt.Errorf("failed to unmarshal _strategy: %w", err)
			}
			d.Match = matches
		default:
			var rows []map[string]any
			if err := yaml.Unmarshal(valueBytes, &rows); err != nil {
				return fmt.Errorf("failed to unmarshal key %s: %w", key, err)
			}
			t := &Table{
				Name: key,
			}
			d.Tables = append(d.Tables, t)
			for _, rowSrc := range rows {
				rowMap := map[string]any{}
				var tags []string
				t.Rows = append(t.Rows, rowMap)
				for k, v := range rowSrc {
					if k == "_tag" {
						switch val := v.(type) {
						case string:
							for _, t := range strings.Split(val, ",") {
								if tt := strings.TrimSpace(t); tt != "" {
									tags = append(tags, tt)
								}
							}
						case []any:
							for _, t := range val {
								if ts, ok := t.(string); ok {
									tags = append(tags, ts)
								} else {
									tags = append(tags, fmt.Sprintf("%v", t))
								}
							}
						default:
							return fmt.Errorf("parse error: tag should be string or [string...], but: '%v'", v)
						}
					} else {
						switch vv := v.(type) {
						case uint64:
							rowMap[k] = int(vv)
						case int64:
							rowMap[k] = int(vv)
						default:
							rowMap[k] = v
						}
					}
				}
				t.Tags = append(t.Tags, tags)
			}
		}
	}

	return nil
}

func filter(src, includes, excludes []string) bool {
	for _, e := range excludes {
		if slices.Contains(src, e) {
			return false
		}
	}
	if len(includes) == 0 {
		return true
	}
	for _, i := range includes {
		if !slices.Contains(src, i) {
			return false
		}
	}
	return true
}

func mapToValues(m map[string]any, primaryKeys []string) ([]Value, error) {
	keys := slices.Sorted(maps.Keys(m))
	row := make([]Value, len(primaryKeys), len(keys))
	var missingPKeys []string
	for i, k := range primaryKeys {
		if v, ok := m[k]; ok {
			row[i].Key = k
			row[i].Value = v
		} else {
			missingPKeys = append(missingPKeys, k)
		}
	}
	if len(missingPKeys) > 0 {
		dump, _ := json.Marshal(m)
		return nil, &ErrMissingPrimaryKey{
			MissingKeys: missingPKeys,
			Dump:        string(dump),
		}
	}
	for _, k := range keys {
		if !slices.Contains(primaryKeys, k) {
			row = append(row, Value{
				Key:   k,
				Value: m[k],
			})
		}
	}
	return row, nil
}

func sortRow(rows [][]Value, sortKeys []string) {
	slices.SortFunc(rows, func(ri, rj []Value) int {
		for i := range sortKeys {
			c := -1
			vi := ri[i].Value
			vj := rj[i].Value
			switch vit := vi.(type) { // todo other types
			case int:
				if vjt, ok := vj.(int); !ok {
					break
				} else {
					c = cmp.Compare(vit, vjt)
				}
			case float64:
				if vjt, ok := vj.(float64); !ok {
					break
				} else {
					c = cmp.Compare(vit, vjt)
				}
			case string:
				if vjt, ok := vj.(string); !ok {
					break
				} else {
					c = cmp.Compare(vit, vjt)
				}
			}
			if c == -1 { // can't convert to primitive
				c = cmp.Compare(fmt.Sprint(vi), fmt.Sprint(vj))
			}
			if c != 0 { // check next primary key
				return c
			}
		}
		return 0 // completely match (should not be happened)
	})
}
