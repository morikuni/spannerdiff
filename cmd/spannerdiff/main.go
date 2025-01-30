package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/blang/semver"
	"github.com/morikuni/aec"
	"github.com/rhysd/go-github-selfupdate/selfupdate"
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
	color := globalFlags.StringP("color", "", "auto", "color mode [auto, always, never] (default: auto)")
	versionFlag := globalFlags.BoolP("version", "", false, "print version")
	updateFlag := globalFlags.BoolP("update", "", false, "update spannerdiff")
	noUpdate := globalFlags.BoolP("no-update", "", false, "disable check for updates")

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

	if *versionFlag {
		fmt.Fprintln(stdout, version)
		return 0
	}

	if *updateFlag {
		return update(stderr)
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

	if !*noUpdate {
		checkUpdate(stderr)
	}

	return 0
}

func checkUpdate(stderr io.Writer) {
	if version == devVersion {
		return
	}

	c, err := readCache()
	if err != nil {
		fmt.Fprintln(stderr, aec.RedF.Apply(fmt.Sprintf("cache error: %v", err)))
		return
	}

	showUpdateFound := func() {
		fmt.Fprintln(stderr, "A new version of spannerdiff is available!\nTo update run:\n $ spannerdiff --update")
	}

	if c.UpdateFound {
		showUpdateFound()
		return
	}

	lastCheck := time.Unix(c.LastCheck, 0)
	if time.Since(lastCheck) < 24*time.Hour {
		return
	}

	v, err := semver.Parse(version)
	if err != nil {
		fmt.Fprintln(stderr, aec.RedF.Apply(fmt.Sprintf("failed to parse version: %v", err)))
		return
	}

	latest, found, err := selfupdate.DetectLatest("morikuni/spannerdiff")
	if err != nil {
		fmt.Fprintln(stderr, aec.RedF.Apply(fmt.Sprintf("failed to check for updates: %v", err)))
		return
	}

	c.LastCheck = time.Now().Unix()

	if !found || latest.Version.LTE(v) {
		if err := writeCache(c); err != nil {
			fmt.Fprintln(stderr, aec.RedF.Apply(fmt.Sprintf("cache error: %v", err)))
			return
		}
		return
	}

	c.UpdateFound = true
	if err := writeCache(c); err != nil {
		fmt.Fprintln(stderr, aec.RedF.Apply(fmt.Sprintf("cache error: %v", err)))
		return
	}
	showUpdateFound()
}

func update(stderr io.Writer) int {
	if version == devVersion {
		fmt.Fprintln(stderr, aec.RedF.Apply("cannot update dev version"))
		return 1
	}

	v, err := semver.Parse(version)
	if err != nil {
		fmt.Fprintln(stderr, aec.RedF.Apply(fmt.Sprintf("failed to parse version: %v", err)))
		return 1
	}

	r, err := selfupdate.UpdateSelf(v, "morikuni/spannerdiff")
	if err != nil {
		fmt.Fprintln(stderr, aec.RedF.Apply(fmt.Sprintf("failed to update: %v", err)))
		return 1
	}
	if r.Version.EQ(v) {
		fmt.Fprintln(stderr, "Already up to date.")
		return 0
	}

	fmt.Fprintln(stderr, "Successfully updated to the latest version!")
	return 0
}

type cache struct {
	UpdateFound bool  `json:"update_found"`
	LastCheck   int64 `json:"last_check"`
}

func cachePath() (string, error) {
	cacheDir, err := os.UserCacheDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(cacheDir, "spannerdiff", "cache.json"), nil
}

func readCache() (*cache, error) {
	cachePath, err := cachePath()
	if err != nil {
		return nil, err
	}
	f, err := os.Open(cachePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return &cache{}, nil
		}
		return nil, err
	}
	defer f.Close()

	var c cache
	if err := json.NewDecoder(f).Decode(&c); err != nil {
		return nil, err
	}
	return &c, nil
}

func writeCache(c *cache) error {
	cachePath, err := cachePath()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(cachePath), 0755); err != nil {
		return err

	}

	f, err := os.Create(cachePath)
	if err != nil {
		return err
	}
	defer f.Close()

	return json.NewEncoder(f).Encode(c)
}
