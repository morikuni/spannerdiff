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

// emulatorForTest returns the emulator. It must be called only when the emulator is configured.
func emulatorForTest(t *testing.T) *emulator {
	t.Helper()
	host := os.Getenv(emulatorHostEnv)
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
	defer func() { _ = resp.Body.Close() }()

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

func (e *emulator) knownFailures() map[string]string {
	return map[string]string{
		"TestDiffColumn/alter_non-stored_generated_column":                                                                "the emulator can't change the expression of a generated column, though Spanner can",
		"TestDiffPropertyGraph/no_change_of_property_graphs_that_Spanner_writes_with_omitted_keys,_labels_and_properties": "the emulator returns property graphs without NO PROPERTIES and the expressions of properties",
	}
}

func (e *emulator) omitsRoles() bool {
	return true
}
