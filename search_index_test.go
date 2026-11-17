package spannerdiff

import "testing"

func TestDiffSearchIndex(t *testing.T) {
	runDiffTests(t, DiffOption{ErrorOnUnsupportedDDL: true}, map[string]diffTest{
		"add search index": {
			``,
			`
			CREATE SEARCH INDEX IDX1 ON T1(T1_S1)`,
			`
			CREATE SEARCH INDEX IDX1 ON T1(T1_S1);`,
			false,
		},
		"add search index on recreated table": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64 NOT NULL,
			  T1_T1 TOKENLIST,
			) PRIMARY KEY(T1_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64 NOT NULL,
			  T1_T1 TOKENLIST,
			) PRIMARY KEY(T1_I1, T1_I2);
			CREATE SEARCH INDEX IDX1 ON T1(T1_T1);`,
			`
			DROP TABLE T1;
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64 NOT NULL,
			  T1_T1 TOKENLIST,
			) PRIMARY KEY(T1_I1, T1_I2);
			CREATE SEARCH INDEX IDX1 ON T1(T1_T1);`,
			false,
		},
		"drop search index": {
			`
			CREATE SEARCH INDEX IDX1 ON T1(T1_S1)`,
			``,
			`
			DROP SEARCH INDEX IDX1;`,
			false,
		},
		"recreate search index": {
			`
			CREATE SEARCH INDEX IDX1 ON T1(T1_I1)`,
			`
			CREATE SEARCH INDEX IDX1 ON T1(T1_I1, T1_S1)`,
			`
			DROP SEARCH INDEX IDX1;
			CREATE SEARCH INDEX IDX1 ON T1(T1_I1, T1_S1);`,
			false,
		},
		"add search index storing": {
			`
			CREATE SEARCH INDEX IDX1 ON T1(T1_S1);`,
			`
			CREATE SEARCH INDEX IDX1 ON T1(T1_S1) STORING (T1_I1);`,
			`
			ALTER SEARCH INDEX IDX1 ADD STORED COLUMN T1_I1;`,
			false,
		},
		"drop search index storing": {
			`
			CREATE SEARCH INDEX IDX1 ON T1(T1_S1) STORING (T1_I1);`,
			`
			CREATE SEARCH INDEX IDX1 ON T1(T1_S1);`,
			`
			ALTER SEARCH INDEX IDX1 DROP STORED COLUMN T1_I1;`,
			false,
		},
		"drop search index storing before column": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_T1 TOKENLIST,
			  T1_S1 STRING(MAX),
			) PRIMARY KEY(T1_I1);
			CREATE SEARCH INDEX SIDX1 ON T1(T1_T1) STORING (T1_S1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_T1 TOKENLIST,
			) PRIMARY KEY(T1_I1);
			CREATE SEARCH INDEX SIDX1 ON T1(T1_T1);`,
			`
			ALTER SEARCH INDEX SIDX1 DROP STORED COLUMN T1_S1;
			ALTER TABLE T1 DROP COLUMN T1_S1;`,
			false,
		},
		"add search index in named schema": {
			``,
			`
			CREATE SEARCH INDEX S1.IDX1 ON S1.T1(T1_S1)`,
			`
			CREATE SEARCH INDEX S1.IDX1 ON S1.T1(T1_S1);`,
			false,
		},
		"move search index to named schema": {
			`
			CREATE SEARCH INDEX IDX1 ON T1(T1_S1)`,
			`
			CREATE SEARCH INDEX S1.IDX1 ON S1.T1(T1_S1)`,
			`
			DROP SEARCH INDEX IDX1;
			CREATE SEARCH INDEX S1.IDX1 ON S1.T1(T1_S1);`,
			false,
		},
	})
}
