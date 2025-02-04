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
	"github.com/mattn/go-isatty"
)

type PrintContext struct {
	Index     int
	TotalSQLs int
}

type Printer interface {
	Print(ctx PrintContext, out io.Writer, sql string) error
}

type printerFunc func(ctx PrintContext, out io.Writer, sql string) error

func (f printerFunc) Print(ctx PrintContext, out io.Writer, sql string) error {
	return f(ctx, out, sql)
}

type NoStylePrinter struct{}

func (NoStylePrinter) Print(ctx PrintContext, out io.Writer, sql string) error {
	_, err := io.WriteString(out, sql)
	return err
}

func WithSpacer(spacer string, p Printer) Printer {
	return printerFunc(func(ctx PrintContext, out io.Writer, sql string) error {
		if ctx.Index != 0 {
			if _, err := io.WriteString(out, spacer); err != nil {
				return err
			}
		}
		return p.Print(ctx, out, sql)
	})
}

type colorPrinter struct {
	lexer     chroma.Lexer
	formatter chroma.Formatter
	style     *chroma.Style
}

func (p colorPrinter) Print(ctx PrintContext, out io.Writer, sql string) error {
	iterator, err := p.lexer.Tokenise(nil, sql)
	if err != nil {
		return fmt.Errorf("failed to tokenize output DDL for colorize: %w", err)
	}
	if err := p.formatter.Format(out, p.style, wrapIterator(iterator)); err != nil {
		return fmt.Errorf("failed to colorize output DDL: %w", err)
	}
	return nil
}

type ColorMode string

const (
	ColorAuto   ColorMode = "auto"
	ColorAlways ColorMode = "always"
	ColorNever  ColorMode = "never"
)

func NewColorMode(s string) (ColorMode, bool) {
	switch ColorMode(s) {
	case ColorAuto, ColorAlways, ColorNever:
		return ColorMode(s), true
	default:
		return "", false
	}
}

func DetectTerminalPrinter(mode ColorMode, stdout *os.File) Printer {
	var p Printer
	switch mode {
	case ColorAlways:
		p = NewColorTerminalPrinter()
	case ColorNever:
		p = NoStylePrinter{}
	case ColorAuto:
		if isatty.IsTerminal(stdout.Fd()) {
			p = NewColorTerminalPrinter()
		} else {
			p = NoStylePrinter{}
		}
	default:
		panic(fmt.Sprintf("unexpected color mode: %s", mode)) // パニックではなくエラーを返すように変更も検討すべき
	}
	return WithSpacer("\n", p)
}

func NewColorTerminalPrinter() Printer {
	lexer := lexers.Get("sql")
	formatter := formatters.Get(detectColorFormatter())
	style, err := chroma.NewXMLStyle(strings.NewReader(defaultStyle))
	if err != nil {
		panic(fmt.Sprintf("failed to load default style: %v", err))
	}
	return colorPrinter{lexer, formatter, style}
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
  <entry type="KeywordType" style="bold #3c9dd0"/>
  <entry type="LiteralString" style="#6dbf6d"/>
  <entry type="LiteralNumber" style="#d1aa44"/>
  <entry type="GenericInserted" style="bold #4caf50"/> <!-- CREATE -->
  <entry type="GenericEmph" style="bold #ffa726"/> <!-- ALTER -->
  <entry type="GenericDeleted" style="bold #f44336"/> <!-- DROP -->
</style>
`

func wrapIterator(iter chroma.Iterator) chroma.Iterator {
	return func() chroma.Token {
		t := iter()
		switch strings.ToUpper(t.Value) {
		case "BOOL", "INT64", "FLOAT32", "FLOAT64", "STRING", "BYTES", "DATE", "TIMESTAMP", "NUMERIC", "JSON", "TOKENLIST", "ARRAY", "STRUCT":
			t.Type = chroma.KeywordType
		case "CREATE", "ADD":
			t.Type = chroma.GenericInserted // fake type for colorize
		case "ALTER", "REPLACE":
			t.Type = chroma.GenericEmph // fake type for colorize
		case "DROP", "DELETE":
			t.Type = chroma.GenericDeleted // fake type for colorize
		default:
			if token.IsKeyword(t.Value) {
				t.Type = chroma.Keyword
			}
		}
		return t
	}
}
