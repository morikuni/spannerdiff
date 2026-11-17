package spannerdiff

import "testing"

func TestDiffIndex(t *testing.T) {
	runDiffTests(t, DiffOption{ErrorOnUnsupportedDDL: true}, map[string]diffTest{
		"add index": {
			``,
			`
			CREATE INDEX IDX1 ON T1(T1_S1)`,
			`
			CREATE INDEX IDX1 ON T1(T1_S1);`,
			false,
		},
		"add index on recreated column": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64,
			) PRIMARY KEY(T1_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 STRING(MAX),
			) PRIMARY KEY(T1_I1);
			CREATE INDEX IDX1 ON T1(T1_I2);`,
			`
			ALTER TABLE T1 DROP COLUMN T1_I2;
			ALTER TABLE T1 ADD COLUMN T1_I2 STRING(MAX);
			CREATE INDEX IDX1 ON T1(T1_I2);`,
			false,
		},
		"drop index": {
			`
			CREATE INDEX IDX1 ON T1(T1_S1)`,
			``,
			`
			DROP INDEX IDX1;`,
			false,
		},
		"recreate index": {
			`
			CREATE INDEX IDX1 ON T1(T1_I1)`,
			`
			CREATE INDEX IDX1 ON T1(T1_I1, T1_S1)`,
			`
			DROP INDEX IDX1;
			CREATE INDEX IDX1 ON T1(T1_I1, T1_S1);`,
			false,
		},
		"add index storing": {
			`
			CREATE INDEX IDX1 ON T1(T1_S1);`,
			`
			CREATE INDEX IDX1 ON T1(T1_S1) STORING (T1_I1);`,
			`
			ALTER INDEX IDX1 ADD STORED COLUMN T1_I1;`,
			false,
		},
		"drop index storing": {
			`
			CREATE INDEX IDX1 ON T1(T1_S1) STORING (T1_I1);`,
			`
			CREATE INDEX IDX1 ON T1(T1_S1);`,
			`
			ALTER INDEX IDX1 DROP STORED COLUMN T1_I1;`,
			false,
		},
		"drop index storing before column": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			  T1_S2 STRING(MAX),
			) PRIMARY KEY(T1_I1);
			CREATE INDEX IDX1 ON T1(T1_S1) STORING (T1_S2);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			) PRIMARY KEY(T1_I1);
			CREATE INDEX IDX1 ON T1(T1_S1);`,
			`
			ALTER INDEX IDX1 DROP STORED COLUMN T1_S2;
			ALTER TABLE T1 DROP COLUMN T1_S2;`,
			false,
		},
		"add expression index": {
			``,
			`
			CREATE INDEX IDX1 ON T1(T1_I1, (T1_S1 != ''))`,
			`
			CREATE INDEX IDX1 ON T1(T1_I1, (T1_S1 != ""));`,
			false,
		},
		"drop expression index": {
			`
			CREATE INDEX IDX1 ON T1(T1_I1, (T1_S1 != ''))`,
			``,
			`
			DROP INDEX IDX1;`,
			false,
		},
		"recreate expression index": {
			`
			CREATE INDEX IDX1 ON T1(T1_I1, (T1_S1 != ''))`,
			`
			CREATE INDEX IDX1 ON T1(T1_I1, (T1_S1 = ''))`,
			`
			DROP INDEX IDX1;
			CREATE INDEX IDX1 ON T1(T1_I1, (T1_S1 = ""));`,
			false,
		},
		"recreate expression index on recreated column": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64,
			) PRIMARY KEY(T1_I1);
			CREATE INDEX IDX1 ON T1((T1_I2 IS NULL));`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 STRING(MAX),
			) PRIMARY KEY(T1_I1);
			CREATE INDEX IDX1 ON T1((T1_I2 IS NULL));`,
			`
			DROP INDEX IDX1;
			ALTER TABLE T1 DROP COLUMN T1_I2;
			ALTER TABLE T1 ADD COLUMN T1_I2 STRING(MAX);
			CREATE INDEX IDX1 ON T1((T1_I2 IS NULL));`,
			false,
		},
		"drop expression index before column": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			) PRIMARY KEY(T1_I1);
			CREATE INDEX IDX1 ON T1((T1_S1 != ''));`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1);`,
			`
			DROP INDEX IDX1;
			ALTER TABLE T1 DROP COLUMN T1_S1;`,
			false,
		},
	})
}
