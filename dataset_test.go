package dbtestify

import (
	"log"
	"strings"
	"testing"

	"github.com/alecthomas/assert/v2"
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func TestLoadYAML(t *testing.T) {
	source := `
user:
- { name: Frank, luckyNumber: 10 }
- { name: Grace, luckyNumber: 12, _tag: [a, b] }
- { name: Heidi, luckyNumber: 14 }
- { name: Ivan, luckyNumber: 16, _tag: b }
`
	data, err := ParseYAML(strings.NewReader(source))
	assert.NoError(t, err)
	normalizedTable, err := data.Tables[0].SortAndFilter([]string{"name"}, nil, nil)
	assert.NoError(t, err)
	assert.Equal(t, &NormalizedTable{
		Name: "user",
		Rows: [][]Value{
			{Value{"name", "Frank"}, Value{"luckyNumber", 10}},
			{Value{"name", "Grace"}, Value{"luckyNumber", 12}},
			{Value{"name", "Heidi"}, Value{"luckyNumber", 14}},
			{Value{"name", "Ivan"}, Value{"luckyNumber", 16}},
		},
	}, normalizedTable)
}

func TestLoadYAMLWithTag(t *testing.T) {
	source := `
user:
- { name: Ivan, luckyNumber: 16, _tag: b }
- { name: Heidi, luckyNumber: 14 }
- { name: Grace, luckyNumber: 12, _tag: [a, b] }
- { name: Frank, luckyNumber: 10 }
`
	data, err := ParseYAML(strings.NewReader(source))
	assert.NoError(t, err)
	// wrong primary key: email is not exists
	_, err = data.Tables[0].SortAndFilter([]string{"email"}, []string{"b"}, []string{"a"})
	assert.Error(t, err)
	normalizedTable, err := data.Tables[0].SortAndFilter([]string{"name"}, []string{"b"}, []string{"a"})
	assert.NoError(t, err)
	assert.Equal(t, &NormalizedTable{
		Name: "user",
		Rows: [][]Value{
			{Value{"name", "Ivan"}, Value{"luckyNumber", 16}},
		},
	}, normalizedTable)
}

func Test_filter(t *testing.T) {
	type args struct {
		src      []string
		includes []string
		excludes []string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "no tag",
			args: args{},
			want: true,
		},
		{
			name: "no include/exclude specified",
			args: args{
				src: []string{"a"},
			},
			want: true,
		},
		{
			name: "include match",
			args: args{
				src:      []string{"a", "b"},
				includes: []string{"a"},
			},
			want: true,
		},
		{
			name: "include not match",
			args: args{
				src:      []string{"a", "b"},
				includes: []string{"c"},
			},
			want: false,
		},
		{
			name: "exclude match",
			args: args{
				src:      []string{"a", "b"},
				excludes: []string{"a"},
			},
			want: false,
		},
		{
			name: "exclude not match",
			args: args{
				src:      []string{"a", "b"},
				excludes: []string{"c"},
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := filter(tt.args.src, tt.args.includes, tt.args.excludes); got != tt.want {
				t.Errorf("filter() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLoadWithOperation(t *testing.T) {
	source := `
_operation:
    user: clear-insert
    accesslog: truncate
    lastlogin: delete
    group: upsert
    history: insert
user:
- { name: Frank, luckyNumber: 10 }
`
	data, err := ParseYAML(strings.NewReader(source))
	assert.NoError(t, err)
	assert.Equal(t, map[string]Operation{
		"user":      ClearInsertOperation,
		"accesslog": TruncateOperation,
		"lastlogin": DeleteOperation,
		"group":     UpsertOperation,
		"history":   InsertOperation,
	}, data.Operation)
}

func TestLoadWithMatchStrategy(t *testing.T) {
	source := `
_match:
    user: exact
    accesslog: sub
user:
- { name: Frank, luckyNumber: 10 }
`
	data, err := ParseYAML(strings.NewReader(source))
	assert.NoError(t, err)
	assert.Equal(t, map[string]MatchStrategy{
		"user":      ExactMatchStrategy,
		"accesslog": SubMatchStrategy,
	}, data.Match)
}
