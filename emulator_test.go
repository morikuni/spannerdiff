package spannerdiff

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cloudspannerecosystem/memefish"
)

// emulatorHostEnv is the environment variable of the REST endpoint of the Spanner emulator, such as localhost:9020.
// The tests verify the generated DDLs on the emulator when it is set.
const emulatorHostEnv = "SPANNER_EMULATOR_REST_HOST"

// emulator is a client of the REST API of the Spanner emulator.
// The REST API is used instead of the client library to avoid adding its dependencies to this module.
type emulator struct {
	instanceURL string
	databaseSeq atomic.Int64
}

var (
	emulatorOnce   sync.Once
	sharedEmulator *emulator
)

// emulatorForTest returns the emulator, or nil if the emulator is not configured.
func emulatorForTest(t *testing.T) *emulator {
	t.Helper()
	host := os.Getenv(emulatorHostEnv)
	if host == "" {
		return nil
	}
	emulatorOnce.Do(func() {
		const project, instance = "spannerdiff", "test"
		projectURL := fmt.Sprintf("http://%s/v1/projects/%s", host, project)
		e := &emulator{instanceURL: projectURL + "/instances/" + instance}
		var err error
		// The emulator may still be starting, such as a service container of CI.
		for range 30 {
			err = e.call(http.MethodPost, projectURL+"/instances", map[string]any{
				"instanceId": instance,
				"instance": map[string]any{
					"config":      fmt.Sprintf("projects/%s/instanceConfigs/emulator-config", project),
					"displayName": instance,
					"nodeCount":   1,
				},
			}, nil)
			var netErr net.Error
			if !errors.As(err, &netErr) {
				break
			}
			time.Sleep(time.Second)
		}
		// The instance remains when the tests run again on the same emulator.
		if err != nil && !strings.Contains(err.Error(), "already exists") {
			t.Fatalf("failed to create an instance on the emulator: %v", err)
		}
		sharedEmulator = e
	})
	if sharedEmulator == nil {
		t.Fatal("the emulator is not available")
	}
	return sharedEmulator
}

// call calls the REST API. An error of a long-running operation is also returned as an error,
// because the emulator completes operations before it responds.
func (e *emulator) call(method, url string, body, result any) error {
	var reader bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&reader).Encode(body); err != nil {
			return err
		}
	}
	req, err := http.NewRequest(method, url, &reader)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	var raw json.RawMessage
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return fmt.Errorf("failed to decode the response of %s %s: %w", method, url, err)
	}
	var status struct {
		Message string `json:"message"`
		Error   *struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(raw, &status); err != nil {
		return err
	}
	switch {
	case status.Error != nil:
		return fmt.Errorf("%s", status.Error.Message)
	case resp.StatusCode >= 400:
		return fmt.Errorf("%s", status.Message)
	}
	if result != nil {
		return json.Unmarshal(raw, result)
	}
	return nil
}

// createDatabase creates a database with the DDLs and returns its URL.
func (e *emulator) createDatabase(ddls []string) (string, error) {
	name := fmt.Sprintf("db%d", e.databaseSeq.Add(1))
	err := e.call(http.MethodPost, e.instanceURL+"/databases", map[string]any{
		"createStatement": fmt.Sprintf("CREATE DATABASE `%s`", name),
		"extraStatements": ddls,
	}, nil)
	if err != nil {
		return "", err
	}
	return e.instanceURL + "/databases/" + name, nil
}

func (e *emulator) dropDatabase(databaseURL string) error {
	return e.call(http.MethodDelete, databaseURL, nil, nil)
}

func (e *emulator) updateDDL(databaseURL string, ddls []string) error {
	return e.call(http.MethodPatch, databaseURL+"/ddl", map[string]any{"statements": ddls}, nil)
}

func (e *emulator) getDDL(databaseURL string) (string, error) {
	var result struct {
		Statements []string `json:"statements"`
	}
	if err := e.call(http.MethodGet, databaseURL+"/ddl", nil, &result); err != nil {
		return "", err
	}
	var sb strings.Builder
	for _, stmt := range result.Statements {
		sb.WriteString(stmt + ";\n")
	}
	return sb.String(), nil
}

// emulatorUnsupported are the tests whose migration the emulator doesn't accept though Spanner is expected to accept it.
var emulatorUnsupported = map[string]string{
	"TestDiffColumn/alter_non-stored_generated_column": "the emulator can't change the expression of a generated column",
}

// verifyOnEmulator applies the base schema and the migration to the emulator,
// and checks that the resulting schema has no difference from the target schema.
// The target schema is also read from the emulator, so that both schemas are written in the same way.
// It is skipped if the emulator doesn't accept the base schema or the target schema,
// because the emulator doesn't support all features of Spanner and some tests use incomplete schemas.
func verifyOnEmulator(t *testing.T, e *emulator, base, migration, target string, option DiffOption) {
	t.Helper()
	if reason, ok := emulatorUnsupported[strings.TrimSuffix(t.Name(), "/emulator")]; ok {
		t.Skip(reason)
	}
	create := func(name, sql string) string {
		ddls, err := splitDDLs(sql)
		if err != nil {
			t.Fatal(err)
		}
		databaseURL, err := e.createDatabase(ddls)
		if err != nil {
			t.Skipf("the emulator doesn't accept the %s schema: %v", name, err)
		}
		t.Cleanup(func() {
			if err := e.dropDatabase(databaseURL); err != nil {
				t.Errorf("failed to drop the database: %v", err)
			}
		})
		return databaseURL
	}
	targetURL := create("target", target)
	baseURL := create("base", base)

	migrationDDLs, err := splitDDLs(migration)
	if err != nil {
		t.Fatal(err)
	}
	if len(migrationDDLs) > 0 {
		if err := e.updateDDL(baseURL, migrationDDLs); err != nil {
			t.Fatalf("the emulator doesn't accept the migration: %v", err)
		}
	}

	migrated, err := e.getDDL(baseURL)
	if err != nil {
		t.Fatal(err)
	}
	want, err := e.getDDL(targetURL)
	if err != nil {
		t.Fatal(err)
	}
	var rest bytes.Buffer
	if err := Diff(strings.NewReader(migrated), strings.NewReader(want), &rest, option); err != nil {
		t.Fatalf("failed to compare the migrated schema with the target schema: %v\nmigrated schema:\n%s\ntarget schema:\n%s", err, migrated, want)
	}
	if rest.Len() > 0 {
		t.Errorf("the migrated schema differs from the target schema:\n%s\nmigrated schema:\n%s\ntarget schema:\n%s", rest.String(), migrated, want)
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
