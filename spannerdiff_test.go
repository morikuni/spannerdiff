package spannerdiff

import (
	"io"
	"strings"
	"testing"

	"github.com/cloudspannerecosystem/memefish"
	"github.com/google/go-cmp/cmp"
)

func TestDiff(t *testing.T) {
	for name, tt := range map[string]struct {
		base      string
		target    string
		wantDDLs  string
		wantError bool
	}{
		"add column": {
			`CREATE TABLE T1 (
			  C_I1 INT64 NOT NULL,
			  C_S1 STRING(MAX)
			) PRIMARY KEY(C_I1)`,
			`CREATE TABLE T1 (
			  C_I1 INT64 NOT NULL,
			  C_S1 STRING(MAX),
			  C_S2 STRING(MAX)
			) PRIMARY KEY(C_I1)`,
			`ALTER TABLE T1 ADD COLUMN C_S2 STRING(MAX);`,
			false,
		},
		"drop column": {
			`CREATE TABLE T1 (
			  C_I1 INT64 NOT NULL,
			  C_S1 STRING(MAX),
			  C_S2 STRING(MAX)
			) PRIMARY KEY(C_I1)`,
			`CREATE TABLE T1 (
			  C_I1 INT64 NOT NULL,
			  C_S1 STRING(MAX)
			) PRIMARY KEY(C_I1)`,
			`ALTER TABLE T1 DROP COLUMN C_S2;`,
			false,
		},
		"modify column": {
			`CREATE TABLE T1 (
			  C_I1 INT64 NOT NULL,
			  C_S1 STRING(MAX),
			  C_S2 STRING(MAX)
			) PRIMARY KEY(C_I1)`,
			`CREATE TABLE T1 (
			  C_I1 INT64 NOT NULL,
			  C_S1 STRING(MAX),
			  C_S2 STRING(100)
			) PRIMARY KEY(C_I1)`,
			`ALTER TABLE T1 ALTER COLUMN C_S2 STRING(100);`,
			false,
		},
		"add index": {
			`CREATE TABLE T1 (
			  C_I1 INT64 NOT NULL,
			  C_S1 STRING(MAX)
			) PRIMARY KEY(C_I1)`,
			`CREATE TABLE T1 (
			  C_I1 INT64 NOT NULL,
			  C_S1 STRING(MAX)
			) PRIMARY KEY(C_I1);
			CREATE INDEX idx_T1_C_S1 ON T1(C_S1)`,
			`CREATE INDEX idx_T1_C_S1 ON T1(C_S1);`,
			false,
		},
		"drop index": {
			`CREATE TABLE T1 (
			  C_I1 INT64 NOT NULL,
			  C_S1 STRING(MAX)
			) PRIMARY KEY(C_I1);
			CREATE INDEX idx_T1_C_S1 ON T1(C_S1)`,
			`CREATE TABLE T1 (
			  C_I1 INT64 NOT NULL,
			  C_S1 STRING(MAX)
			) PRIMARY KEY(C_I1)`,
			`DROP INDEX idx_T1_C_S1;`,
			false,
		},
		"add search index": {
			`CREATE TABLE T1 (
			  C_I1 INT64 NOT NULL,
			  C_S1 STRING(MAX)
			) PRIMARY KEY(C_I1)`,
			`CREATE TABLE T1 (
			  C_I1 INT64 NOT NULL,
			  C_S1 STRING(MAX)
			) PRIMARY KEY(C_I1);
			CREATE SEARCH INDEX idx_T1_C_S1 ON T1(C_S1)`,
			`CREATE SEARCH INDEX idx_T1_C_S1 ON T1(C_S1);`,
			false,
		},
		"drop search index": {
			`CREATE TABLE T1 (
			  C_I1 INT64 NOT NULL,
			  C_S1 STRING(MAX)
			) PRIMARY KEY(C_I1);
			CREATE SEARCH INDEX idx_T1_C_S1 ON T1(C_S1)`,
			`CREATE TABLE T1 (
			  C_I1 INT64 NOT NULL,
			  C_S1 STRING(MAX)
			) PRIMARY KEY(C_I1)`,
			`DROP SEARCH INDEX idx_T1_C_S1;`,
			false,
		},
	} {
		t.Run(name, func(t *testing.T) {
			r, err := Diff(strings.NewReader(tt.base), strings.NewReader(tt.target), DiffOption{
				ErrorOnUnsupportedDDL: true,
			})
			if (err != nil) != tt.wantError {
				t.Fatalf("want error %v, got %v", tt.wantError, err)
			}
			bs, err := io.ReadAll(r)
			if err != nil {
				t.Fatalf("failed to read diff: %v", err)
			}

			equalDDLs(t, tt.wantDDLs, string(bs))
		})
	}
}

func equalDDLs(t *testing.T, a, b string) {
	//
	ddlsA, err := memefish.ParseDDLs("a", a)
	if err != nil {
		t.Fatalf("failed to parse ddl a: %v", err)
	}
	ddlsB, err := memefish.ParseDDLs("b", b)
	if err != nil {
		t.Fatalf("failed to parse ddl b: %v", err)
	}
	linesA := make([]string, 0, len(ddlsA))
	for _, ddl := range ddlsA {
		linesA = append(linesA, ddl.SQL())
	}
	linesB := make([]string, 0, len(ddlsB))
	for _, ddl := range ddlsB {
		linesB = append(linesB, ddl.SQL())
	}
	if diff := cmp.Diff(linesA, linesB); diff != "" {
		t.Errorf("diff (-got +want):\n%s", diff)
	}
}
