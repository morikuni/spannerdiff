package spannerdiff

import (
	"bytes"
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
		"unsupported ddl": {
			``,
			`ALTER INDEX IDX1 ADD STORED COLUMN T1_I1;`,
			``,
			true,
		},
		"add schema": {
			``,
			`CREATE SCHEMA S1;`,
			`CREATE SCHEMA S1;`,
			false,
		},
		"drop schema": {
			`CREATE SCHEMA S1;`,
			``,
			`DROP SCHEMA S1;`,
			false,
		},
		"add table": {
			``,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1);`,
			false,
		},
		"drop table": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1)`,
			``,
			`DROP TABLE T1;`,
			false,
		},
		"recreate table": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1, T1_S1)`,
			`
			DROP TABLE T1;
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1, T1_S1);`,
			false,
		},
		"recreate dependencies by recreate table": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			  T1_AF1 ARRAY<FLOAT64> NOT NULL,
			) PRIMARY KEY (T1_I1);
			CREATE INDEX IDX1 ON T1(T1_I1);
			CREATE SEARCH INDEX IDX2 ON T1(T1_S1);
			CREATE CHANGE STREAM S1 FOR ALL;
			CREATE CHANGE STREAM S2 FOR T1;
			CREATE VECTOR INDEX IDX3 ON T1(T1_AF1) OPTIONS (distance_type = 'COSINE');`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			  T1_AF1 ARRAY<FLOAT64> NOT NULL,
			) PRIMARY KEY (T1_S1);
			CREATE INDEX IDX1 ON T1(T1_I1);
			CREATE SEARCH INDEX IDX2 ON T1(T1_S1);
			CREATE CHANGE STREAM S1 FOR ALL;
			CREATE CHANGE STREAM S2 FOR T1;
			CREATE VECTOR INDEX IDX3 ON T1(T1_AF1) OPTIONS (distance_type = 'COSINE');`,
			`
			DROP VECTOR INDEX IDX3;
			DROP SEARCH INDEX IDX2;
			DROP INDEX IDX1;
			ALTER CHANGE STREAM S2 DROP FOR ALL;
			DROP TABLE T1;
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			  T1_AF1 ARRAY<FLOAT64> NOT NULL,
			) PRIMARY KEY (T1_S1);
			ALTER CHANGE STREAM S2 SET FOR T1;
			CREATE INDEX IDX1 ON T1(T1_I1);
			CREATE SEARCH INDEX IDX2 ON T1(T1_S1);
			CREATE VECTOR INDEX IDX3 ON T1(T1_AF1) OPTIONS (distance_type = 'COSINE');`,
			false,
		},
		"add column": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			) PRIMARY KEY(T1_I1)`,
			`ALTER TABLE T1 ADD COLUMN T1_S1 STRING(MAX);`,
			false,
		},
		"drop column": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			) PRIMARY KEY(T1_I1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1)`,
			`ALTER TABLE T1 DROP COLUMN T1_S1;`,
			false,
		},
		"alter column": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			) PRIMARY KEY(T1_I1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(100),
			) PRIMARY KEY(T1_I1)`,
			`ALTER TABLE T1 ALTER COLUMN T1_S1 STRING(100);`,
			false,
		},
		"recreate column": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			) PRIMARY KEY(T1_I1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 INT64,
			) PRIMARY KEY(T1_I1)`,
			`
			ALTER TABLE T1 DROP COLUMN T1_S1;
			ALTER TABLE T1 ADD COLUMN T1_S1 INT64;`,
			false,
		},
		"add index": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX)
			) PRIMARY KEY(T1_I1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX)
			) PRIMARY KEY(T1_I1);
			CREATE INDEX IDX1 ON T1(T1_S1)`,
			`CREATE INDEX IDX1 ON T1(T1_S1);`,
			false,
		},
		"drop index": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX)
			) PRIMARY KEY(T1_I1);
			CREATE INDEX IDX1 ON T1(T1_S1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX)
			) PRIMARY KEY(T1_I1)`,
			`DROP INDEX IDX1;`,
			false,
		},
		"recreate index": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX)
			) PRIMARY KEY(T1_I1);
			CREATE INDEX IDX1 ON T1(T1_I1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX)
			) PRIMARY KEY(T1_I1);
			CREATE INDEX IDX1 ON T1(T1_I1, T1_S1)`,
			`
			DROP INDEX IDX1;
			CREATE INDEX IDX1 ON T1(T1_I1, T1_S1);`,
			false,
		},
		"add index storing": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX)
			) PRIMARY KEY(T1_I1);
			CREATE INDEX IDX1 ON T1(T1_S1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX)
			) PRIMARY KEY(T1_I1);
			CREATE INDEX IDX1 ON T1(T1_S1) STORING (T1_I1);`,
			`ALTER INDEX IDX1 ADD STORED COLUMN T1_I1;`,
			false,
		},
		"drop index storing": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX)
			) PRIMARY KEY(T1_I1);
			CREATE INDEX IDX1 ON T1(T1_S1) STORING (T1_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX)
			) PRIMARY KEY(T1_I1);
			CREATE INDEX IDX1 ON T1(T1_S1);`,
			`ALTER INDEX IDX1 DROP STORED COLUMN T1_I1;`,
			false,
		},
		"add search index": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX)
			) PRIMARY KEY(T1_I1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX)
			) PRIMARY KEY(T1_I1);
			CREATE SEARCH INDEX IDX1 ON T1(T1_S1)`,
			`CREATE SEARCH INDEX IDX1 ON T1(T1_S1);`,
			false,
		},
		"drop search index": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX)
			) PRIMARY KEY(T1_I1);
			CREATE SEARCH INDEX IDX1 ON T1(T1_S1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX)
			) PRIMARY KEY(T1_I1)`,
			`DROP SEARCH INDEX IDX1;`,
			false,
		},
		"recreate search index": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX)
			) PRIMARY KEY(T1_I1);
			CREATE SEARCH INDEX IDX1 ON T1(T1_I1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX)
			) PRIMARY KEY(T1_I1);
			CREATE SEARCH INDEX IDX1 ON T1(T1_I1, T1_S1)`,
			`
			DROP SEARCH INDEX IDX1;
			CREATE SEARCH INDEX IDX1 ON T1(T1_I1, T1_S1);`,
			false,
		},
		"add search index storing": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX)
			) PRIMARY KEY(T1_I1);
			CREATE SEARCH INDEX IDX1 ON T1(T1_S1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX)
			) PRIMARY KEY(T1_I1);
			CREATE SEARCH INDEX IDX1 ON T1(T1_S1) STORING (T1_I1);`,
			`ALTER SEARCH INDEX IDX1 ADD STORED COLUMN T1_I1;`,
			false,
		},
		"drop search index storing": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX)
			) PRIMARY KEY(T1_I1);
			CREATE SEARCH INDEX IDX1 ON T1(T1_S1) STORING (T1_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX)
			) PRIMARY KEY(T1_I1);
			CREATE SEARCH INDEX IDX1 ON T1(T1_S1);`,
			`ALTER SEARCH INDEX IDX1 DROP STORED COLUMN T1_I1;`,
			false,
		},
		"add vector index": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_AF1 ARRAY<FLOAT64> NOT NULL,
			) PRIMARY KEY(T1_I1);
			`,
			`
			CREATE TABLE T1 (
			T1_I1 INT64 NOT NULL,
			T1_AF1 ARRAY<FLOAT64> NOT NULL,
			) PRIMARY KEY(T1_I1);
			CREATE VECTOR INDEX IDX1 ON T1(T1_AF1) OPTIONS (distance_type = 'COSINE');`,
			`CREATE VECTOR INDEX IDX1 ON T1(T1_AF1) OPTIONS (distance_type = 'COSINE');`,
			false,
		},
		"drop vector index": {
			`
			CREATE TABLE T1 (
			T1_I1 INT64 NOT NULL,
			T1_AF1 ARRAY<FLOAT64> NOT NULL,
			) PRIMARY KEY(T1_I1);
			CREATE VECTOR INDEX IDX1 ON T1(T1_AF1) OPTIONS (distance_type = 'COSINE');`,
			`
			CREATE TABLE T1 (
			T1_I1 INT64 NOT NULL,
			T1_AF1 ARRAY<FLOAT64> NOT NULL,
			) PRIMARY KEY(T1_I1);
			`,
			`DROP VECTOR INDEX IDX1;`,
			false,
		},
		"recreate vector index": {
			`
			CREATE TABLE T1 (
			T1_I1 INT64 NOT NULL,
			T1_AF1 ARRAY<FLOAT64> NOT NULL,
			) PRIMARY KEY(T1_I1);
			CREATE VECTOR INDEX IDX1 ON T1(T1_AF1) OPTIONS (distance_type = 'COSINE');`,
			`
			CREATE TABLE T1 (
			T1_I1 INT64 NOT NULL,
			T1_AF1 ARRAY<FLOAT64> NOT NULL,
			) PRIMARY KEY(T1_I1);
			CREATE VECTOR INDEX IDX1 ON T1(T1_AF1) OPTIONS (distance_type = 'EUCLIDEAN');`,
			`
			DROP VECTOR INDEX IDX1;
			CREATE VECTOR INDEX IDX1 ON T1(T1_AF1) OPTIONS (distance_type = 'EUCLIDEAN');`,
			false,
		},
		"add foreign key": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX)
			) PRIMARY KEY(T1_I1);
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			  T2_S1 STRING(MAX)
			) PRIMARY KEY(T2_I1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX)
			) PRIMARY KEY(T1_I1);
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			  T2_S1 STRING(MAX),
			  CONSTRAINT FK1 FOREIGN KEY (T2_S1) REFERENCES T1 (T1_S1),
			) PRIMARY KEY(T2_I1);
			`,
			`
			ALTER TABLE T2 ADD CONSTRAINT FK1 FOREIGN KEY (T2_S1) REFERENCES T1(T1_S1);`,
			false,
		},
		"drop foreign key": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX)
			) PRIMARY KEY(T1_I1);
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			  T2_S1 STRING(MAX),
			  CONSTRAINT FK1 FOREIGN KEY (T2_S1) REFERENCES T1 (T1_S1),
			) PRIMARY KEY(T2_I1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX)
			) PRIMARY KEY(T1_I1);
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			  T2_S1 STRING(MAX)
			) PRIMARY KEY(T2_I1)`,
			`
			ALTER TABLE T2 DROP CONSTRAINT FK1;`,
			false,
		},
		"recreate foreign key": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			) PRIMARY KEY(T1_I1);
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			  T2_S1 STRING(MAX),
			  CONSTRAINT FK1 FOREIGN KEY (T2_I1) REFERENCES T1 (T1_I1),
			) PRIMARY KEY(T2_I1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			) PRIMARY KEY(T1_I1);
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			  T2_S1 STRING(MAX),
			  CONSTRAINT FK1 FOREIGN KEY (T2_S1) REFERENCES T1 (T1_S1),
			) PRIMARY KEY(T2_I1)`,
			`
			ALTER TABLE T2 DROP CONSTRAINT FK1;
			ALTER TABLE T2 ADD CONSTRAINT FK1 FOREIGN KEY (T2_S1) REFERENCES T1(T1_S1);`,
			false,
		},
		"add check constraint": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  CONSTRAINT CHK1 CHECK (T1_I1 > 0)
			) PRIMARY KEY(T1_I1)`,
			`ALTER TABLE T1 ADD CONSTRAINT CHK1 CHECK (T1_I1 > 0);`,
			false,
		},
		"drop check constraint": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  CONSTRAINT CHK1 CHECK (T1_I1 > 0)
			) PRIMARY KEY(T1_I1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1)`,
			`ALTER TABLE T1 DROP CONSTRAINT CHK1;`,
			false,
		},
		"recreate check constraint": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  CONSTRAINT CHK1 CHECK (T1_I1 > 0)
			) PRIMARY KEY(T1_I1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  CONSTRAINT CHK1 CHECK (T1_I1 > 1)
			) PRIMARY KEY(T1_I1)`,
			`
			ALTER TABLE T1 DROP CONSTRAINT CHK1;
			ALTER TABLE T1 ADD CONSTRAINT CHK1 CHECK (T1_I1 > 1);`,
			false,
		},
		"add row deletion policy": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_TS1 TIMESTAMP NOT NULL,
			) PRIMARY KEY(T1_I1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_TS1 TIMESTAMP NOT NULL,
			) PRIMARY KEY(T1_I1), ROW DELETION POLICY (OLDER_THAN(T1_TS1, INTERVAL 1 DAY));`,
			`ALTER TABLE T1 ADD ROW DELETION POLICY (OLDER_THAN(T1_TS1, INTERVAL 1 DAY));`,
			false,
		},
		"drop row deletion policy": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_TS1 TIMESTAMP NOT NULL,
			) PRIMARY KEY(T1_I1), ROW DELETION POLICY (OLDER_THAN(T1_TS1, INTERVAL 1 DAY));`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_TS1 TIMESTAMP NOT NULL,
			) PRIMARY KEY(T1_I1)`,
			`ALTER TABLE T1 DROP ROW DELETION POLICY;`,
			false,
		},
		"replace row deletion policy": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_TS1 TIMESTAMP NOT NULL,
			) PRIMARY KEY(T1_I1), ROW DELETION POLICY (OLDER_THAN(T1_TS1, INTERVAL 1 DAY));`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_TS1 TIMESTAMP NOT NULL,
			) PRIMARY KEY(T1_I1), ROW DELETION POLICY (OLDER_THAN(T1_TS1, INTERVAL 2 DAY));`,
			`
			ALTER TABLE T1 REPLACE ROW DELETION POLICY (OLDER_THAN(T1_TS1, INTERVAL 2 DAY));`,
			false,
		},
		"add synonym": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY (T1_I1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  SYNONYM(T2)
			) PRIMARY KEY (T1_I1)`,
			`ALTER TABLE T1 ADD SYNONYM T2;`,
			false,
		},
		"drop synonym": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  SYNONYM(T2)
			) PRIMARY KEY (T1_I1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY (T1_I1)`,
			`ALTER TABLE T1 DROP SYNONYM T2;`,
			false,
		},
		"recreate synonym": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  SYNONYM(T2)
			) PRIMARY KEY (T1_I1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  SYNONYM(T3)
			) PRIMARY KEY (T1_I1)`,
			`
			ALTER TABLE T1 ADD SYNONYM T3;
			ALTER TABLE T1 DROP SYNONYM T2;`,
			false,
		},
		"add property graph": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY (T1_I1);
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			) PRIMARY KEY (T2_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY (T1_I1);
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			) PRIMARY KEY (T2_I1);
			CREATE PROPERTY GRAPH G1 NODE TABLES (T1, T2);
			`,
			`
			CREATE PROPERTY GRAPH G1 NODE TABLES (T1, T2);`,
			false,
		},
		"drop property graph": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY (T1_I1);
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			) PRIMARY KEY (T2_I1);
			CREATE PROPERTY GRAPH G1 NODE TABLES (T1, T2);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY (T1_I1);
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			) PRIMARY KEY (T2_I1);`,
			`
			DROP PROPERTY GRAPH G1;`,
			false,
		},
		"recreate property graph": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY (T1_I1);
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			) PRIMARY KEY (T2_I1);
			CREATE PROPERTY GRAPH G1 NODE TABLES (T1, T2);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY (T1_I1);
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			) PRIMARY KEY (T2_I1);
			CREATE PROPERTY GRAPH G1 NODE TABLES (T1);`,
			`
			CREATE OR REPLACE PROPERTY GRAPH G1 NODE TABLES (T1);`,
			false,
		},
		"create view": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY (T1_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY (T1_I1);
			CREATE VIEW V1 SQL SECURITY DEFINER AS SELECT * FROM T1;`,
			`CREATE VIEW V1 SQL SECURITY DEFINER AS SELECT * FROM T1;`,
			false,
		},
		"drop view": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY (T1_I1);
			CREATE VIEW V1 SQL SECURITY DEFINER AS SELECT * FROM T1;`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY (T1_I1);`,
			`DROP VIEW V1;`,
			false,
		},
		"recreate view": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY (T1_I1);
			CREATE VIEW V1 SQL SECURITY DEFINER AS SELECT * FROM T1;`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY (T1_I1);
			CREATE VIEW V1 SQL SECURITY DEFINER AS SELECT * FROM T1 WHERE T1_I1 > 0;`,
			`
			CREATE OR REPLACE VIEW V1 SQL SECURITY DEFINER AS SELECT * FROM T1 WHERE T1_I1 > 0;`,
			false,
		},
		"add change stream": {
			``,
			`
			CREATE CHANGE STREAM S1 FOR ALL;`,
			`CREATE CHANGE STREAM S1 FOR ALL;`,
			false,
		},
		"drop change stream": {
			`
			CREATE CHANGE STREAM S1 FOR ALL;`,
			``,
			`DROP CHANGE STREAM S1;`,
			false,
		},
		"alter change stream": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY (T1_I1);
			CREATE CHANGE STREAM S1 FOR ALL OPTIONS ( retention_period = '36h' );`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY (T1_I1);
			CREATE CHANGE STREAM S1 FOR T1(T1_I1) OPTIONS ( retention_period = '72h' );`,
			`
			ALTER CHANGE STREAM S1 SET FOR T1(T1_I1);
			ALTER CHANGE STREAM S1 SET OPTIONS ( retention_period = '72h' );`,
			false,
		},
		"add sequence": {
			``,
			`
			CREATE SEQUENCE S1 OPTIONS (sequence_kind = 'bit_reversed_positive');`,
			`CREATE SEQUENCE S1 OPTIONS (sequence_kind = 'bit_reversed_positive');`,
			false,
		},
		"drop sequence": {
			`
			CREATE SEQUENCE S1 OPTIONS (sequence_kind = 'bit_reversed_positive');`,
			``,
			`DROP SEQUENCE S1;`,
			false,
		},
		"alter sequence": {
			`
			CREATE SEQUENCE S1 OPTIONS (skip_range_min = 1000, skip_range_max = 2000);`,
			`
			CREATE SEQUENCE S1 OPTIONS (start_counter_with = 10);`,
			`
			ALTER SEQUENCE S1 SET OPTIONS (start_counter_with = 10);`,
			false,
		},
	} {
		t.Run(name, func(t *testing.T) {
			var buf bytes.Buffer
			err := Diff(strings.NewReader(tt.base), strings.NewReader(tt.target), &buf, DiffOption{
				ErrorOnUnsupportedDDL: true,
			})
			if tt.wantError {
				if err == nil {
					t.Fatalf("want error, got nil")
				}
				return
			} else if err != nil {
				t.Fatalf("want no error, got %v", err)
			}

			if (err != nil) != tt.wantError {
				t.Fatalf("want error %v, got %v", tt.wantError, err)
			}
			equalDDLs(t, tt.wantDDLs, buf.String())
		})
	}
}

func equalDDLs(t *testing.T, a, b string) {
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
