package spannerdiff

import (
	"bytes"
	"strings"
	"testing"

	"github.com/cloudspannerecosystem/memefish"
	"github.com/google/go-cmp/cmp"
)

type diffTest struct {
	base      string
	target    string
	wantDDLs  string
	wantError bool
}

func runDiffTests(t *testing.T, option DiffOption, tests map[string]diffTest) {
	t.Helper()
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			var buf bytes.Buffer
			err := Diff(strings.NewReader(tt.base), strings.NewReader(tt.target), &buf, option)
			if tt.wantError {
				if err == nil {
					t.Fatalf("want error, got nil")
				}
				return
			} else if err != nil {
				t.Fatalf("want no error, got %v", err)
			}
			equalDDLs(t, tt.wantDDLs, buf.String())
		})
	}
}

func TestDiff(t *testing.T) {
	runDiffTests(t, DiffOption{ErrorOnUnsupportedDDL: true}, map[string]diffTest{
		"unsupported ddl": {
			``,
			`
			ALTER INDEX IDX1 ADD STORED COLUMN T1_I1;`,
			``,
			true,
		},
		"identifiers are case-insensitive": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX) OPTIONS (allow_commit_timestamp = false),
			  CONSTRAINT C1 CHECK (T1_I1 > 0),
			  SYNONYM(S1),
			) PRIMARY KEY(T1_I1);
			CREATE INDEX IDX1 ON T1(T1_S1) STORING (T1_I1);
			CREATE INDEX IDX2 ON T1((T1_S1 != ''));
			CREATE VIEW V1 SQL SECURITY INVOKER AS SELECT T1.T1_I1 FROM T1;
			CREATE QUEUE Q1 (
			  T1_I1 INT64 NOT NULL,
			  Payload BYTES(MAX) NOT NULL,
			) PRIMARY KEY(T1_I1), INTERLEAVE IN PARENT T1, OPTIONS (receive_mode = 'PULL');
			GRANT SELECT(T1_S1) ON TABLE T1 TO ROLE R1;`,
			`
			create table t1 (
			  t1_i1 int64 not null,
			  t1_s1 string(max) options (ALLOW_COMMIT_TIMESTAMP = false),
			  constraint c1 check (t1_i1 > 0),
			  synonym(s1),
			) primary key(t1_i1);
			create index idx1 on t1(t1_s1) storing (t1_i1);
			create index idx2 on t1((t1_s1 != ''));
			create view v1 sql security invoker as select t1.t1_i1 from t1;
			create queue q1 (
			  t1_i1 int64 not null,
			  payload bytes(max) not null,
			) primary key(t1_i1), interleave in parent t1, options (RECEIVE_MODE = 'PULL');
			grant select(t1_s1) on table t1 to role r1;`,
			``,
			false,
		},
		"proto type names are case-sensitive": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_P1 examples.Message,
			) PRIMARY KEY(T1_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_P1 examples.message,
			) PRIMARY KEY(T1_I1);`,
			`
			ALTER TABLE T1 ALTER COLUMN T1_P1 examples.message;`,
			false,
		},
		"ignore IF NOT EXISTS and OR REPLACE": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1);
			CREATE INDEX IDX1 ON T1(T1_I1);
			CREATE SEQUENCE S1 OPTIONS (sequence_kind = 'bit_reversed_positive');
			CREATE VIEW V1 SQL SECURITY INVOKER AS SELECT T1.T1_I1 FROM T1;`,
			`
			CREATE TABLE IF NOT EXISTS T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1);
			CREATE INDEX IF NOT EXISTS IDX1 ON T1(T1_I1);
			CREATE SEQUENCE IF NOT EXISTS S1 OPTIONS (sequence_kind = 'bit_reversed_positive');
			CREATE OR REPLACE VIEW V1 SQL SECURITY INVOKER AS SELECT T1.T1_I1 FROM T1;`,
			``,
			false,
		},
		"issue #35": { // https://github.com/morikuni/spannerdiff/issues/35
			``,
			`
			CREATE OR REPLACE VIEW V2 SQL SECURITY INVOKER AS SELECT * FROM T1;
			CREATE OR REPLACE VIEW V1 SQL SECURITY INVOKER AS SELECT * FROM V2;`,
			`
			CREATE OR REPLACE VIEW V2 SQL SECURITY INVOKER AS SELECT * FROM T1;
			CREATE OR REPLACE VIEW V1 SQL SECURITY INVOKER AS SELECT * FROM V2;`,
			false,
		},
		"issue #37": { // https://github.com/morikuni/spannerdiff/issues/37
			`
			CREATE INDEX IDX1 ON T1 (T1_I1)`,
			`
			CREATE INDEX IDX1 ON T1 (T1_I1 ASC)`,
			``,
			false,
		},
	})
}

func equalDDLs(t *testing.T, a, b string) {
	t.Helper()
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
		t.Errorf("diff (+got -want):\n%s", diff)
	}
}

func TestDiffDeterministic(t *testing.T) {
	for name, tt := range map[string]struct {
		base   string
		target string
	}{
		"constraints, synonyms, row deletion policy and index": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64 NOT NULL,
			  T1_I3 INT64 NOT NULL,
			  T1_I4 INT64 NOT NULL,
			  T1_T1 TIMESTAMP NOT NULL,
			  CONSTRAINT C1 CHECK (T1_I1 > 0),
			  CONSTRAINT C2 CHECK (T1_I2 > 0),
			  SYNONYM(S1),
			) PRIMARY KEY(T1_I1);
			CREATE INDEX IDX1 ON T1(T1_I2) STORING (T1_I3, T1_I4);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64 NOT NULL,
			  T1_I3 INT64 NOT NULL,
			  T1_I4 INT64 NOT NULL,
			  T1_T1 TIMESTAMP NOT NULL,
			  CONSTRAINT C3 CHECK (T1_I3 > 0),
			  CONSTRAINT C4 CHECK (T1_I4 > 0),
			  SYNONYM(S2),
			) PRIMARY KEY(T1_I1), ROW DELETION POLICY (OLDER_THAN(T1_T1, INTERVAL 1 DAY));
			CREATE INDEX IDX1 ON T1(T1_I2) STORING (T1_T1, T1_I1);`,
		},
		"foreign keys with on delete": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			  T1_S2 STRING(MAX),
			  CONSTRAINT FK1 FOREIGN KEY (T1_S1) REFERENCES T2 (T2_S1),
			  CONSTRAINT FK2 FOREIGN KEY (T1_S1) REFERENCES T2 (T2_S1) ON DELETE CASCADE,
			  FOREIGN KEY (T1_S2) REFERENCES T2 (T2_S1),
			  FOREIGN KEY (T1_S2) REFERENCES T2 (T2_S1) ON DELETE CASCADE,
			) PRIMARY KEY(T1_I1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			  T1_S2 STRING(MAX),
			  FOREIGN KEY (T1_S1) REFERENCES T2 (T2_S1) ON DELETE NO ACTION,
			  FOREIGN KEY (T1_S1) REFERENCES T2 (T2_S1) ON DELETE CASCADE,
			  FOREIGN KEY (T1_S2) REFERENCES T2 (T2_S1) ON DELETE NO ACTION,
			  FOREIGN KEY (T1_S2) REFERENCES T2 (T2_S1) ON DELETE CASCADE,
			  CONSTRAINT FK3 FOREIGN KEY (T1_S2) REFERENCES T2 (T2_S1) ON DELETE NO ACTION NOT ENFORCED,
			) PRIMARY KEY(T1_I1)`,
		},
		"constraints by alter table": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64,
			  T1_I3 INT64,
			) PRIMARY KEY(T1_I1);
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			  T2_I2 INT64,
			) PRIMARY KEY(T2_I1);
			CREATE TABLE T3 (
			  T3_I1 INT64 NOT NULL,
			  T3_I2 INT64,
			) PRIMARY KEY(T3_I1);
			ALTER TABLE T1 ADD CONSTRAINT FK1 FOREIGN KEY (T1_I2) REFERENCES T2 (T2_I1);
			ALTER TABLE T1 ADD FOREIGN KEY (T1_I3) REFERENCES T3 (T3_I1);
			ALTER TABLE T2 ADD CONSTRAINT FK2 FOREIGN KEY (T2_I2) REFERENCES T3 (T3_I1);
			ALTER TABLE T3 ADD CONSTRAINT C1 CHECK (T3_I2 > 0);`,
			`
			CREATE TABLE T1 (
			  T1_I1 STRING(MAX) NOT NULL,
			  T1_I2 INT64,
			  T1_I3 INT64,
			) PRIMARY KEY(T1_I1);
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			  T2_I2 INT64,
			  T2_I3 INT64 NOT NULL,
			) PRIMARY KEY(T2_I1, T2_I3);
			CREATE TABLE T3 (
			  T3_I1 INT64 NOT NULL,
			  T3_I2 INT64,
			) PRIMARY KEY(T3_I1);
			CREATE TABLE T4 (
			  T4_I1 INT64 NOT NULL,
			  T4_S1 STRING(MAX),
			) PRIMARY KEY(T4_I1);
			ALTER TABLE T1 ADD CONSTRAINT FK1 FOREIGN KEY (T1_I2) REFERENCES T2 (T2_I1);
			ALTER TABLE T1 ADD FOREIGN KEY (T1_I3) REFERENCES T3 (T3_I1);
			ALTER TABLE T2 ADD CONSTRAINT FK2 FOREIGN KEY (T2_I2) REFERENCES T3 (T3_I1);
			ALTER TABLE T3 ADD CONSTRAINT C2 CHECK (T3_I2 > 0);
			ALTER TABLE T4 ADD FOREIGN KEY (T4_S1) REFERENCES T1 (T1_I1);`,
		},
		"change HIDDEN of column in recreated table": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64 NOT NULL,
			  T1_I3 INT64,
			) PRIMARY KEY(T1_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64 NOT NULL,
			  T1_I3 INT64 HIDDEN,
			) PRIMARY KEY(T1_I1, T1_I2);`,
		},
		"convert column to identity column in recreated table": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64 NOT NULL,
			  T1_I3 INT64,
			) PRIMARY KEY(T1_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64 NOT NULL,
			  T1_I3 INT64 GENERATED BY DEFAULT AS IDENTITY (BIT_REVERSED_POSITIVE),
			) PRIMARY KEY(T1_I1, T1_I2);`,
		},
	} {
		t.Run(name, func(t *testing.T) {
			var first string
			for i := range 50 {
				var buf bytes.Buffer
				if err := Diff(strings.NewReader(tt.base), strings.NewReader(tt.target), &buf, DiffOption{}); err != nil {
					t.Fatal(err)
				}
				if i == 0 {
					first = buf.String()
					continue
				}
				if diff := cmp.Diff(first, buf.String()); diff != "" {
					t.Fatalf("output changed between runs:\n%s", diff)
				}
			}
		})
	}
}

func TestDiffIndependentOfAlterTableOrder(t *testing.T) {
	tables := `
	CREATE TABLE T1 (
	  T1_I1 INT64 NOT NULL,
	) PRIMARY KEY(T1_I1);
	CREATE TABLE T2 (
	  T2_I1 INT64 NOT NULL,
	) PRIMARY KEY(T2_I1);
	CREATE TABLE T3 (
	  T3_I1 INT64 NOT NULL,
	) PRIMARY KEY(T3_I1);`
	fk2 := `ALTER TABLE T1 ADD FOREIGN KEY (T1_I1) REFERENCES T2 (T2_I1);`
	fk3 := `ALTER TABLE T1 ADD FOREIGN KEY (T1_I1) REFERENCES T3 (T3_I1);`

	var outputs []string
	for _, base := range []string{tables + fk2 + fk3, tables + fk3 + fk2} {
		var buf bytes.Buffer
		if err := Diff(strings.NewReader(base), strings.NewReader(""), &buf, DiffOption{}); err != nil {
			t.Fatal(err)
		}
		outputs = append(outputs, buf.String())
	}
	if diff := cmp.Diff(outputs[0], outputs[1]); diff != "" {
		t.Fatalf("output changed by the order of ALTER TABLE:\n%s", diff)
	}
}

func TestDiffErrorOnUnsupportedAlterTable(t *testing.T) {
	var buf bytes.Buffer
	err := Diff(strings.NewReader(``), strings.NewReader(`ALTER TABLE S1.S2.T1 ADD CHECK (T1_I1 > 0)`), &buf, DiffOption{
		ErrorOnUnsupportedDDL: true,
	})
	if err == nil || !strings.Contains(err.Error(), "unsupported DDL: ALTER TABLE S1.S2.T1 ADD CHECK (T1_I1 > 0)") {
		t.Fatalf("want unsupported DDL error, got %v", err)
	}
}

func TestDiffOnUnsupportedDDL(t *testing.T) {
	var ignored []string
	var buf bytes.Buffer
	target := `
	CREATE TABLE T1 (
	  T1_I1 INT64 NOT NULL,
	) PRIMARY KEY(T1_I1);
	ALTER TABLE T1 ADD CHECK (T1_I1 > 0);
	ALTER TABLE S1.S2.T1 ADD CHECK (T1_I1 > 0);
	ALTER INDEX IDX1 ADD STORED COLUMN T1_I1`
	err := Diff(strings.NewReader(``), strings.NewReader(target), &buf, DiffOption{
		OnUnsupportedDDL: func(sql string) {
			ignored = append(ignored, sql)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"ALTER TABLE S1.S2.T1 ADD CHECK (T1_I1 > 0)",
		"ALTER INDEX IDX1 ADD STORED COLUMN T1_I1",
	}
	if diff := cmp.Diff(want, ignored); diff != "" {
		t.Errorf("diff (+got -want):\n%s", diff)
	}
}
