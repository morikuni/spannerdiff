package spannerdiff

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cloudspannerecosystem/memefish"
)

// omniContainerEnv is the environment variable of the name of the Docker container that runs Spanner Omni.
// The tests verify the generated DDLs on Spanner Omni when it is set.
const omniContainerEnv = "SPANNER_OMNI_CONTAINER"

// omni runs the CLI of Spanner Omni in its container.
// Spanner Omni has no REST API, and the CLI is used instead of the client library
// to avoid adding its dependencies to this module.
type omni struct {
	container   string
	databaseSeq atomic.Int64
}

var sharedOmni = &omni{container: os.Getenv(omniContainerEnv)}

// omniForTest returns Spanner Omni, or nil if Spanner Omni is not configured.
func omniForTest() *omni {
	if sharedOmni.container == "" {
		return nil
	}
	return sharedOmni
}

// omniRun makes the names of databases different from those of the previous runs on the same container.
var omniRun = time.Now().Unix()

// run runs the CLI with the DDLs as the file of --ddl-file, if any.
func (o *omni) run(ddls []string, args ...string) (string, error) {
	// The default timeout of the CLI is too short when many schema changes run in parallel.
	args = append([]string{"exec", "-i", o.container, "/google/spanner/bin/spanner"}, append(args, "--timeout", "30m")...)
	cmd := exec.Command("docker", args...)
	if len(ddls) > 0 {
		cmd.Args = append(cmd.Args, "--ddl-file", "/dev/stdin")
		cmd.Stdin = strings.NewReader(strings.Join(ddls, ";\n") + ";\n")
	}
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		// The error follows the warning about the license.
		msg := stderr.String()
		if i := strings.Index(msg, "Error: "); i >= 0 {
			msg = msg[i+len("Error: "):]
		}
		return "", fmt.Errorf("%s: %w", strings.TrimSpace(msg), err)
	}
	// The CLI overwrites the line of its progress by the result.
	out := stdout.String()
	if i := strings.LastIndex(out, "\x1b[K"); i >= 0 {
		out = out[i+len("\x1b[K"):]
	}
	return out, nil
}

// createDatabase creates a database with the DDLs and returns its name.
// The databases are not dropped, because the container is removed after the tests.
func (o *omni) createDatabase(ddls []string) (string, error) {
	name := fmt.Sprintf("db%d-%d", omniRun, o.databaseSeq.Add(1))
	if _, err := o.run(ddls, "databases", "create", name); err != nil {
		return "", err
	}
	return name, nil
}

func (o *omni) updateDDL(database string, ddls []string) error {
	_, err := o.run(ddls, "databases", "ddl", "update", database)
	return err
}

func (o *omni) getDDL(database string) (string, error) {
	out, err := o.run(nil, "databases", "ddl", "describe", database)
	if err != nil {
		return "", err
	}
	if strings.HasPrefix(out, "Listed 0 items.") {
		return "", nil
	}
	return out, nil
}

// omniKnownDifferences are the tests whose target schema Spanner Omni writes differently,
// so the migrated schema read from Spanner Omni has a difference from the target schema.
var omniKnownDifferences = map[string]string{
	"TestDiffPropertyGraph/add_property_graph_on_recreated_table": "Spanner writes the keys, labels and properties that the definition of a property graph omits",
}

// verifyOnOmni applies the base schema and the migration to Spanner Omni,
// and checks that the resulting schema has no difference from the target schema.
// A database of the target schema is not created to compare, because Spanner Omni slows down
// as the number of databases increases.
// It is skipped if Spanner Omni doesn't accept the base schema or the target schema,
// because Spanner Omni doesn't support all features of Spanner and some tests use incomplete schemas.
func verifyOnOmni(t *testing.T, o *omni, base, migration, target string, option DiffOption) {
	t.Helper()
	if reason, ok := omniKnownDifferences[strings.TrimSuffix(t.Name(), "/omni")]; ok {
		t.Skip(reason)
	}
	create := func(sql string) (string, error) {
		ddls, err := splitDDLs(sql)
		if err != nil {
			t.Fatal(err)
		}
		return o.createDatabase(ddls)
	}
	database, err := create(base)
	if err != nil {
		t.Skipf("Spanner Omni doesn't accept the base schema: %v", err)
	}

	migrationDDLs, err := splitDDLs(migration)
	if err != nil {
		t.Fatal(err)
	}
	if len(migrationDDLs) > 0 {
		if err := o.updateDDL(database, migrationDDLs); err != nil {
			// The migration is also rejected when Spanner Omni doesn't accept the target schema.
			if _, targetErr := create(target); targetErr != nil {
				t.Skipf("Spanner Omni doesn't accept the target schema: %v", targetErr)
			}
			t.Fatalf("Spanner Omni doesn't accept the migration: %v", err)
		}
	}

	migrated, err := o.getDDL(database)
	if err != nil {
		t.Fatal(err)
	}
	var rest bytes.Buffer
	if err := Diff(strings.NewReader(migrated), strings.NewReader(target), &rest, option); err != nil {
		t.Fatalf("failed to compare the migrated schema with the target schema: %v\nmigrated schema:\n%s", err, migrated)
	}
	if rest.Len() > 0 {
		t.Errorf("the migrated schema differs from the target schema:\n%s\nmigrated schema:\n%s", rest.String(), migrated)
	}
}

func splitDDLs(sql string) ([]string, error) {
	ddls, err := memefish.ParseDDLs("", sql)
	if err != nil {
		return nil, err
	}
	stmts := make([]string, 0, len(ddls))
	for _, ddl := range ddls {
		stmts = append(stmts, ddl.SQL())
	}
	return stmts, nil
}
