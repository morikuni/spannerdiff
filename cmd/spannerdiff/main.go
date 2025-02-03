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
		fmt.Fprintf(stderr, `%s:
      spannerdiff [flags] [base-flags] [target-flags]

%s:
%s
%s:
%s
%s:
%s
%s:
      > $ spanerdiff --base "CREATE TABLE t1 (c1 INT64) PRIMARY KEY(c1)" --target "CREATE TABLE t1 (c1 INT64, c2 INT64) PRIMARY KEY (c1)"
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

	if *versionFlag {
		fmt.Fprintln(stdout, version)
		return 0
	}

	if *baseStdin && *targetStdin {
		fmt.Fprintln(stderr, aec.RedF.Apply("cannot specify both --base-stdin and --target-stdin"))
		return 1
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
			fmt.Fprintln(stderr, aec.RedF.Apply(fmt.Sprintf("failed to open base DDL file: %v", err)))
			return 2
		}
		defer f.Close()
		base = f
	}
	if *targetFile != "" {
		f, err := os.Open(*targetFile)
		if err != nil {
			fmt.Fprintln(stderr, aec.RedF.Apply(fmt.Sprintf("failed to open target DDL file: %v", err)))
			return 2
		}
		defer f.Close()
		target = f
	}
	if base == nil && *baseDDL == "" && target == nil && *targetDDL == "" {
		fmt.Fprintln(stderr, aec.YellowF.Apply("both base and target schema are not specified"))
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
