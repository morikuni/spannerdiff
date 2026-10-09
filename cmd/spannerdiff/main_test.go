package main

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRealMain(t *testing.T) {
	dir := t.TempDir()
	schemaFile := filepath.Join(dir, "schema.sql")
	if err := os.WriteFile(schemaFile, []byte("CREATE TABLE T1 (T1_I1 INT64) PRIMARY KEY (T1_I1)"), 0o600); err != nil {
		t.Fatal(err)
	}

	for name, tt := range map[string]struct {
		args       []string
		stdin      string
		wantCode   int
		wantStdout string
		wantStderr string
	}{
		"diff": {
			args:       []string{"--base", "", "--target", "CREATE SCHEMA S1"},
			wantCode:   0,
			wantStdout: "CREATE SCHEMA S1;\n",
		},
		"target file": {
			args:       []string{"--target-file", schemaFile},
			wantCode:   0,
			wantStdout: "CREATE TABLE T1 (\n  T1_I1 INT64\n) PRIMARY KEY (T1_I1);\n",
		},
		"base stdin": {
			args:       []string{"--base-stdin", "--target", ""},
			stdin:      "CREATE SCHEMA S1",
			wantCode:   0,
			wantStdout: "DROP SCHEMA S1;\n",
		},
		"invalid color": {
			args:       []string{"--color", "foo"},
			wantCode:   2,
			wantStderr: "invalid color mode: foo",
		},
		"multiple base sources": {
			args:       []string{"--base", "", "--base-file", schemaFile},
			wantCode:   2,
			wantStderr: "only one of --base, --base-file and --base-stdin can be specified",
		},
		"multiple target sources": {
			args:       []string{"--target-stdin", "--target-file", schemaFile},
			wantCode:   2,
			wantStderr: "only one of --target, --target-file and --target-stdin can be specified",
		},
		"both stdin": {
			args:       []string{"--base-stdin", "--target-stdin"},
			wantCode:   2,
			wantStderr: "cannot specify both --base-stdin and --target-stdin",
		},
		"file not found": {
			args:       []string{"--base-file", filepath.Join(dir, "not-found.sql")},
			wantCode:   2,
			wantStderr: "failed to open base DDL file",
		},
		"unsupported ddl": {
			args:       []string{"--target", "ALTER INDEX IDX1 ADD STORED COLUMN C1"},
			wantCode:   0,
			wantStderr: "ignored unsupported DDL: ALTER INDEX IDX1 ADD STORED COLUMN C1",
		},
		"error on unsupported ddl": {
			args:       []string{"--error-on-unsupported-ddl", "--target", "ALTER INDEX IDX1 ADD STORED COLUMN C1"},
			wantCode:   1,
			wantStderr: "unsupported DDL: ALTER INDEX IDX1 ADD STORED COLUMN C1",
		},
		"parse error": {
			args:       []string{"--target", "CREATE"},
			wantCode:   1,
			wantStderr: "failed to parse target SQL",
		},
	} {
		t.Run(name, func(t *testing.T) {
			stdout, err := os.CreateTemp(t.TempDir(), "stdout")
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				_ = stdout.Close()
			}()
			var stderr bytes.Buffer

			code := realMain(append([]string{"spannerdiff", "--color", "never"}, tt.args...), strings.NewReader(tt.stdin), stdout, &stderr)
			if code != tt.wantCode {
				t.Errorf("want exit code %d, got %d: %s", tt.wantCode, code, stderr.String())
			}

			if _, err := stdout.Seek(0, io.SeekStart); err != nil {
				t.Fatal(err)
			}
			gotStdout, err := io.ReadAll(stdout)
			if err != nil {
				t.Fatal(err)
			}
			if string(gotStdout) != tt.wantStdout {
				t.Errorf("want stdout %q, got %q", tt.wantStdout, gotStdout)
			}
			if !strings.Contains(stderr.String(), tt.wantStderr) {
				t.Errorf("want stderr to contain %q, got %q", tt.wantStderr, stderr.String())
			}
		})
	}
}
