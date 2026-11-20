package spannerdiff

import (
	"bytes"
	"os"
	"strings"
	"testing"

	"github.com/cloudspannerecosystem/memefish"
	"github.com/cloudspannerecosystem/memefish/ast"
)

// spannerServer is a Spanner to verify the generated DDLs on: the emulator or Spanner Omni.
// The emulator is fast, but it doesn't support some features and rejects some DDLs that Spanner accepts.
// Spanner Omni is Spanner itself, but a schema change takes tens of seconds.
type spannerServer interface {
	// createDatabase creates a database with the DDLs and returns the value to specify it.
	createDatabase(ddls []string) (string, error)
	dropDatabase(database string) error
	updateDDL(database string, ddls []string) error
	getDDL(database string) (string, error)
	// omitsRoles reports whether getDDL doesn't return roles and grants.
	omitsRoles() bool
	// knownFailures returns the reasons of the tests that fail on the server though they are expected to pass on Spanner.
	knownFailures() map[string]string
}

// spannerForTest returns the Spanner to verify the generated DDLs on, or nil if no Spanner is configured.
func spannerForTest(t *testing.T) spannerServer {
	t.Helper()
	switch {
	case os.Getenv(omniContainerEnv) != "":
		return omniForTest(t)
	case os.Getenv(emulatorHostEnv) != "":
		return emulatorForTest(t)
	default:
		return nil
	}
}

// knownDifferences are the tests whose target schema Spanner writes differently,
// so the migrated schema read from Spanner has a difference from the target schema.
var knownDifferences = map[string]string{
	"TestDiffPropertyGraph/add_property_graph_on_recreated_table": "Spanner writes the keys, labels and properties that the definition of a property graph omits",
}

// verifyOnSpanner applies the base schema and the migration to Spanner,
// and checks that the resulting schema has no difference from the target schema.
// It is skipped if Spanner doesn't accept the base schema or the target schema,
// because the emulator and Spanner Omni don't support all features of Spanner and some tests use incomplete schemas.
func verifyOnSpanner(t *testing.T, spanner spannerServer, base, migration, target string, option DiffOption) {
	t.Helper()
	name := strings.TrimSuffix(t.Name(), "/spanner")
	if reason, ok := spanner.knownFailures()[name]; ok {
		t.Skip(reason)
	}
	create := func(sql string) (string, error) {
		ddls, err := splitDDLs(sql, false)
		if err != nil {
			t.Fatal(err)
		}
		database, err := spanner.createDatabase(ddls)
		if err != nil {
			return "", err
		}
		t.Cleanup(func() {
			if err := spanner.dropDatabase(database); err != nil {
				t.Errorf("failed to drop the database: %v", err)
			}
		})
		return database, nil
	}
	database, err := create(base)
	if err != nil {
		t.Skipf("the base schema is not accepted: %v", err)
	}

	migrationDDLs, err := splitDDLs(migration, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(migrationDDLs) > 0 {
		if err := spanner.updateDDL(database, migrationDDLs); err != nil {
			// The migration is also rejected when the target schema is not accepted.
			if _, targetErr := create(target); targetErr != nil {
				t.Skipf("the target schema is not accepted: %v", targetErr)
			}
			t.Fatalf("the migration is not accepted: %v", err)
		}
	}

	if reason, ok := knownDifferences[name]; ok {
		t.Skip(reason)
	}
	migrated, err := spanner.getDDL(database)
	if err != nil {
		t.Fatal(err)
	}
	// Roles and grants are only checked to be accepted, if Spanner doesn't return them.
	targetDDLs, err := splitDDLs(target, spanner.omitsRoles())
	if err != nil {
		t.Fatal(err)
	}
	want := strings.Join(targetDDLs, ";\n")
	var rest bytes.Buffer
	if err := Diff(strings.NewReader(migrated), strings.NewReader(want), &rest, option); err != nil {
		t.Fatalf("failed to compare the migrated schema with the target schema: %v\nmigrated schema:\n%s", err, migrated)
	}
	if rest.Len() > 0 {
		t.Errorf("the migrated schema differs from the target schema:\n%s\nmigrated schema:\n%s", rest.String(), migrated)
	}
}

// splitDDLs returns the statements of the SQL, without those of roles and grants if withoutRoles is true.
func splitDDLs(sql string, withoutRoles bool) ([]string, error) {
	ddls, err := memefish.ParseDDLs("", sql)
	if err != nil {
		return nil, err
	}
	stmts := make([]string, 0, len(ddls))
	for _, ddl := range ddls {
		switch ddl.(type) {
		case *ast.CreateRole, *ast.Grant:
			if withoutRoles {
				continue
			}
		}
		stmts = append(stmts, ddl.SQL())
	}
	return stmts, nil
}
