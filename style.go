package spannerdiff

import (
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/alecthomas/chroma/v2"
	"github.com/alecthomas/chroma/v2/formatters"
	"github.com/alecthomas/chroma/v2/lexers"
	"github.com/cloudspannerecosystem/memefish/token"
)

type printer func(out io.Writer, sql string) error

func newPrinter(colorize bool) printer {
	if colorize {
		return chromaPrinter()
	}

	return noStyle
}

func noStyle(out io.Writer, sql string) error {
	_, err := io.WriteString(out, sql)
	return err
}

func chromaPrinter() printer {
	lexer := lexers.Get("sql")
	formatter := formatters.Get(detectColorFormatter())
	style, err := chroma.NewXMLStyle(strings.NewReader(defaultStyle))
	if err != nil {
		panic(fmt.Sprintf("failed to load default style: %v", err))
	}
	return func(out io.Writer, sql string) error {
		iterator, err := lexer.Tokenise(nil, sql)
		if err != nil {
			return fmt.Errorf("failed to tokenize output DDL for colorize: %w", err)
		}
		if err := formatter.Format(out, style, wrapIterator(iterator)); err != nil {
			return fmt.Errorf("failed to colorize output DDL: %w", err)
		}
		return nil
	}
}

func detectColorFormatter() string {
	if os.Getenv("COLORTERM") == "truecolor" {
		return "terminal16m"
	}

	term := os.Getenv("TERM")
	switch {
	case strings.Contains(term, "256color"):
		return "terminal256"
	case strings.Contains(term, "16color"):
		return "terminal16"
	case strings.Contains(term, "color"):
		return "terminal8"
	default:
		return "terminal8"
	}
}

const defaultStyle = `
<style name="default">
  <entry type="Keyword" style="bold #4482d1"/>
  <entry type="KeywordType" style="bold #3c9dd0"/> <!-- 視認性を高める明るい青 -->
  <entry type="LiteralString" style="#6dbf6d"/>
  <entry type="LiteralNumber" style="#d1aa44"/>
  <entry type="GenericInserted" style="bold #4caf50"/> <!-- CREATE -->
  <entry type="GenericHeading" style="bold #ffa726"/> <!-- ALTER -->
  <entry type="GenericDeleted" style="bold #f44336"/> <!-- DROP -->
</style>
`

func wrapIterator(iter chroma.Iterator) chroma.Iterator {
	return func() chroma.Token {
		t := iter()
		switch strings.ToUpper(t.Value) {
		case "BOOL", "INT64", "FLOAT32", "FLOAT64", "STRING", "BYTES", "DATE", "TIMESTAMP", "NUMERIC", "JSON", "TOKENLIST", "ARRAY", "STRUCT":
			t.Type = chroma.KeywordType
		case "CREATE":
			t.Type = chroma.GenericInserted // fake type for colorize
		case "ALTER", "REPLACE":
			t.Type = chroma.GenericHeading // fake type for colorize
		case "DROP":
			t.Type = chroma.GenericDeleted // fake type for colorize
		default:
			if token.IsKeyword(t.Value) {
				t.Type = chroma.Keyword
			}
		}
		return t
	}
}
