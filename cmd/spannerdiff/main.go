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

const devVersion = "dev"

var version = devVersion

func realMain(args []string, stdin io.Reader, stdout *os.File, stderr io.Writer) int {
	globalFlags := pflag.NewFlagSet("", pflag.ContinueOnError)
	globalFlags.SortFlags = false
	color := globalFlags.StringP("color", "", "auto", "color mode [auto, always, never]")
	errorOnUnsupportedDDL := globalFlags.BoolP("error-on-unsupported-ddl", "", false, "exit with an error if the schema contains unsupported DDL instead of ignoring it")
	versionFlag := globalFlags.BoolP("version", "", false, "print version")

	baseFlags := pflag.NewFlagSet("", pflag.ContinueOnError)
	baseFlags.SortFlags = false
	baseDDL := baseFlags.StringP("base", "", "", "base schema")
	baseFile := baseFlags.StringP("base-file", "", "", "read base schema from file")
	baseStdin := baseFlags.BoolP("base-stdin", "", false, "read base schema from stdin")

	targetFlags := pflag.NewFlagSet("", pflag.ContinueOnError)
	targetFlags.SortFlags = false
	targetDDL := targetFlags.StringP("target", "", "", "target schema")
	targetFile := targetFlags.StringP("target-file", "", "", "read target schema from file")
	targetStdin := targetFlags.BoolP("target-stdin", "", false, "read target schema from stdin")

	rootFlags := pflag.NewFlagSet(args[0], pflag.ContinueOnError)
	rootFlags.SortFlags = false
	rootFlags.AddFlagSet(globalFlags)
	rootFlags.AddFlagSet(baseFlags)
	rootFlags.AddFlagSet(targetFlags)

	rootFlags.Usage = func() {
		_, _ = fmt.Fprintf(stderr, `%s:
      spannerdiff [flags] [base-flags] [target-flags]

%s:
%s
%s:
%s
%s:
%s
%s:
      > $ spannerdiff --base "CREATE TABLE t1 (c1 INT64) PRIMARY KEY(c1)" --target "CREATE TABLE t1 (c1 INT64, c2 INT64) PRIMARY KEY (c1)"
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
		_, _ = fmt.Fprintln(stderr, aec.RedF.Apply(err.Error()))
		return 2
	}

	if *versionFlag {
		_, _ = fmt.Fprintln(stdout, version)
		return 0
	}

	usageError := func(msg string) int {
		_, _ = fmt.Fprintln(stderr, aec.RedF.Apply(msg))
		return 2
	}

	cm, ok := spannerdiff.NewColorMode(*color)
	if !ok {
		return usageError(fmt.Sprintf("invalid color mode: %s", *color))
	}

	if countTrue(rootFlags.Changed("base"), *baseFile != "", *baseStdin) > 1 {
		return usageError("only one of --base, --base-file and --base-stdin can be specified")
	}
	if countTrue(rootFlags.Changed("target"), *targetFile != "", *targetStdin) > 1 {
		return usageError("only one of --target, --target-file and --target-stdin can be specified")
	}
	if *baseStdin && *targetStdin {
		return usageError("cannot specify both --base-stdin and --target-stdin")
	}

	var base, target io.Reader
	if *baseStdin {
		base = stdin
	}
	if *targetStdin {
		target = stdin
	}
	if *baseFile != "" {
		f, err := os.Open(*baseFile)
		if err != nil {
			return usageError(fmt.Sprintf("failed to open base DDL file: %v", err))
		}
		defer func() {
			_ = f.Close()
		}()
		base = f
	}
	if *targetFile != "" {
		f, err := os.Open(*targetFile)
		if err != nil {
			return usageError(fmt.Sprintf("failed to open target DDL file: %v", err))
		}
		defer func() {
			_ = f.Close()
		}()
		target = f
	}
	if base == nil && *baseDDL == "" && target == nil && *targetDDL == "" {
		_, _ = fmt.Fprintln(stderr, aec.YellowF.Apply("both base and target schema are not specified"))
	}
	if base == nil {
		base = strings.NewReader(*baseDDL)
	}
	if target == nil {
		target = strings.NewReader(*targetDDL)
	}

	err := spannerdiff.Diff(base, target, stdout, spannerdiff.DiffOption{
		ErrorOnUnsupportedDDL: *errorOnUnsupportedDDL,
		OnUnsupportedDDL: func(sql string) {
			_, _ = fmt.Fprintln(stderr, aec.YellowF.Apply(fmt.Sprintf("ignored unsupported DDL: %s", sql)))
		},
		Printer: spannerdiff.DetectTerminalPrinter(cm, stdout),
	})
	if err != nil {
		_, _ = fmt.Fprintln(stderr, aec.RedF.Apply(err.Error()))
		return 1
	}

	return 0
}

func countTrue(bs ...bool) int {
	var n int
	for _, b := range bs {
		if b {
			n++
		}
	}
	return n
}
