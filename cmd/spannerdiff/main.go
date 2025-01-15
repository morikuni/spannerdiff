package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/morikuni/aec"

	"github.com/morikuni/spannerdiff"
)

func main() {
	os.Exit(realMain(os.Args, os.Stdin, os.Stdout, os.Stderr))
}

func realMain(args []string, stdin io.Reader, stdout *os.File, stderr io.Writer) int {
	fs := flag.NewFlagSet(args[0], flag.ContinueOnError)
	baseDDLFile := fs.String("base-ddl-file", "", "read base schema from file")
	baseFromStdin := fs.Bool("base-from-stdin", false, "read base schema from stdin")
	baseDDL := fs.String("base-ddl", "", "base schema DDL")
	targetDDLFile := fs.String("target-ddl-file", "", "read target schema from file")
	targetFromStdin := fs.Bool("target-from-stdin", false, "read target schema from stdin")
	targetDDL := fs.String("target-ddl", "", "target schema DDL")
	color := fs.String("color", "auto", "colorize output. [auto, always, never]")

	if err := fs.Parse(args[1:]); err != nil {
		fmt.Fprintln(stderr, aec.RedF.Apply(err.Error()))
		return 2
	}

	if *baseFromStdin && *targetFromStdin {
		fmt.Fprintln(stderr, aec.RedF.Apply("cannot specify both --base-from-stdin and --target-from-stdin"))
		return 1
	}

	var base, target io.Reader
	if *baseFromStdin {
		base = stdin
	}
	if *targetFromStdin {
		target = stdin
	}
	if *baseDDLFile != "" {
		f, err := os.Open(*baseDDLFile)
		if err != nil {
			fmt.Fprintln(stderr, aec.RedF.Apply(fmt.Sprintf("failed to open base DDL file: %v", err)))
			return 2
		}
		defer f.Close()
		base = f
	}
	if *targetDDLFile != "" {
		f, err := os.Open(*targetDDLFile)
		if err != nil {
			fmt.Fprintln(stderr, aec.RedF.Apply(fmt.Sprintf("failed to open target DDL file: %v", err)))
			return 2
		}
		defer f.Close()
		target = f
	}
	if base == nil {
		base = strings.NewReader(*baseDDL)
	}
	if target == nil {
		target = strings.NewReader(*targetDDL)
	}

	cm, ok := spannerdiff.NewColorMode(*color)
	if !ok {
		fmt.Fprintln(stderr, aec.RedF.Apply(fmt.Sprintf("invalid color mode: %s", *color)))
	}
	err := spannerdiff.Diff(base, target, stdout, spannerdiff.DiffOption{
		Printer: spannerdiff.DetectTerminalPrinter(cm, stdout),
	})
	if err != nil {
		fmt.Fprintln(stderr, aec.RedF.Apply(err.Error()))
		return 1
	}
	return 0
}
