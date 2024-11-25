package search

import (
	"encoding/json"

	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/snapshot/vfs"
	"github.com/alecthomas/participle/v2"
	"github.com/alecthomas/participle/v2/lexer"
)

var customLexer = lexer.Must(lexer.New(
	lexer.Rules{
		"Root": {
			{Name: "Whitespace", Pattern: `\s+`, Action: nil},
			{Name: "Number", Pattern: `-?\d+(\.\d+)?`, Action: nil},
			{Name: "Operator", Pattern: `(?:!=|<>|<=|>=|<|>|:|=|~=|~)`, Action: nil},
			{Name: "DotDelimitedValue", Pattern: `[a-zA-Z0-9_]+(\.[a-zA-Z0-9_]+)+`, Action: nil}, // Dot-delimited values (e.g., "report.pdf")
			{Name: "Ident", Pattern: `[a-zA-Z_][a-zA-Z0-9_]*`, Action: nil},                      // Identifiers
			{Name: "QuotedString", Pattern: `"[^"]*"`, Action: nil},
			{Name: "Error", Pattern: `.`, Action: nil},
		},
	},
))

type Result interface {
	isSearchResult()
}

type FileEntry struct {
	Repository string           `json:"repository"`
	Snapshot   objects.Checksum `json:"snapshot"`
	FileEntry  vfs.FileEntry    `json:"fileentry"`
}

func (FileEntry) isSearchResult() {}

// / Query represents the full query structure.
type Query struct {
	Left     *Filter `@@`                            // Left-hand side of the query
	Operator *string `[ @( "AND" | "OR" | "NOT" ) ]` // Logical operator (optional)
	Right    *Query  `@@?`                           // Right-hand side of the query (optional)
}

func (q *Query) String() string {
	buf, err := json.Marshal(q)
	if err != nil {
		return ""
	}
	return string(buf)
}

// Filter represents a single field filter.
type Filter struct {
	Field    string `@Ident`
	Operator string `@Operator`
	Value    string `(@Number | @QuotedString | @DotDelimitedValue | @Ident)` // Value (quoted, dot-delimited, number, or identifier)
}

func (f *Filter) String() string {
	buf, err := json.Marshal(f)
	if err != nil {
		return ""
	}
	return string(buf)
}

func Parse(query string) (*Query, error) {
	parser := participle.MustBuild[Query](
		participle.Lexer(customLexer), // Use the custom lexer
		participle.Elide("Whitespace"),
	)
	return parser.ParseString("", query)
}
