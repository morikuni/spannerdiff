package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/morikuni/aec"
	"github.com/spf13/pflag"

	"github.com/morikuni/spannerdiff"
)

func main() {
	os.Exit(realMain(os.Args, os.Stdin, os.Stdout, os.Stderr))
}

func realMain(args []string, stdin io.Reader, stdout *os.File, stderr io.Writer) int {
	globalFlags := pflag.NewFlagSet("", pflag.ContinueOnError)
	globalFlags.SortFlags = false
	color := globalFlags.StringP("color", "", "auto", "colorize output. [auto, always, never]")

	baseFlags := pflag.NewFlagSet("", pflag.ContinueOnError)
	baseFlags.SortFlags = false
	baseDDLFile := baseFlags.StringP("base-ddl-file", "", "", "read base schema from file")
	baseFromStdin := baseFlags.BoolP("base-from-stdin", "", false, "read base schema from stdin")
	baseDDL := baseFlags.StringP("base-ddl", "", "", "base schema")

	targetFlags := pflag.NewFlagSet("", pflag.ContinueOnError)
	targetFlags.SortFlags = false
	targetDDLFile := targetFlags.StringP("target-ddl-file", "", "", "read target schema from file")
	targetFromStdin := targetFlags.BoolP("target-from-stdin", "", false, "read target schema from stdin")
	targetDDL := targetFlags.StringP("target-ddl", "", "", "target schema")

	rootFlags := pflag.NewFlagSet(args[0], pflag.ContinueOnError)
	rootFlags.SortFlags = false
	rootFlags.AddFlagSet(globalFlags)
	rootFlags.AddFlagSet(baseFlags)
	rootFlags.AddFlagSet(targetFlags)

	rootFlags.Usage = func() {
		fmt.Fprintf(stderr, `%s:
      spannerdiff [flags] [base-flags] [target-flags]

%s:
%s
%s:
%s
%s:
%s
%s:		
      > $ spanerdiff --base-ddl "CREATE TABLE t1 (c1 INT64) PRIMARY KEY(c1)" --target-ddl "CREATE TABLE t1 (c1 INT64, c2 INT64) PRIMARY KEY (c1)"
      > ALTER TABLE t1 ADD COLUMN c2 INT64;
`,
			aec.Bold.Apply("Usage"),
			aec.Bold.Apply("Flags"),
			globalFlags.FlagUsages(),
			aec.Bold.Apply("Base Flags"),
			baseFlags.FlagUsages(),
			aec.Bold.Apply("Target Flags"),
			targetFlags.FlagUsages(),
			aec.Bold.Apply("Example"),
		)
	}

	if err := rootFlags.Parse(args[1:]); err != nil {
		if errors.Is(err, pflag.ErrHelp) {
			return 0
		}
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
