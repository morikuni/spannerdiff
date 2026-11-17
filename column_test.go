package spannerdiff

import "testing"

func TestDiffColumn(t *testing.T) {
	runDiffTests(t, DiffOption{ErrorOnUnsupportedDDL: true}, map[string]diffTest{
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
			`
			ALTER TABLE T1 ADD COLUMN T1_S1 STRING(MAX);`,
			false,
		},
		"add not null column": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX) NOT NULL,
			) PRIMARY KEY(T1_I1)`,
			``,
			true,
		},
		"add not null column with default": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX) NOT NULL DEFAULT ('a'),
			) PRIMARY KEY(T1_I1)`,
			`
			ALTER TABLE T1 ADD COLUMN T1_S1 STRING(MAX) NOT NULL DEFAULT ('a');`,
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
			`
			ALTER TABLE T1 DROP COLUMN T1_S1;`,
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
			`
			ALTER TABLE T1 ALTER COLUMN T1_S1 STRING(100);`,
			false,
		},
		"remove column options": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_T1 TIMESTAMP OPTIONS (allow_commit_timestamp = true),
			) PRIMARY KEY(T1_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_T1 TIMESTAMP,
			) PRIMARY KEY(T1_I1);`,
			`
			ALTER TABLE T1 ALTER COLUMN T1_T1 SET OPTIONS (allow_commit_timestamp = null);`,
			false,
		},
		"allow commit timestamp on column with default value": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_T1 TIMESTAMP NOT NULL DEFAULT (CURRENT_TIMESTAMP()),
			) PRIMARY KEY(T1_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_T1 TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
			) PRIMARY KEY(T1_I1);`,
			`
			ALTER TABLE T1 ALTER COLUMN T1_T1 DROP DEFAULT;
			ALTER TABLE T1 ALTER COLUMN T1_T1 SET OPTIONS (allow_commit_timestamp = true);`,
			false,
		},
		"disallow commit timestamp and set default value": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_T1 TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
			) PRIMARY KEY(T1_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_T1 TIMESTAMP NOT NULL DEFAULT (CURRENT_TIMESTAMP()),
			) PRIMARY KEY(T1_I1);`,
			`
			ALTER TABLE T1 ALTER COLUMN T1_T1 SET OPTIONS (allow_commit_timestamp = null);
			ALTER TABLE T1 ALTER COLUMN T1_T1 SET DEFAULT (CURRENT_TIMESTAMP());`,
			false,
		},
		"allow commit timestamp and change default value to pending commit timestamp": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_T1 TIMESTAMP DEFAULT (CURRENT_TIMESTAMP()),
			) PRIMARY KEY(T1_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_T1 TIMESTAMP DEFAULT (PENDING_COMMIT_TIMESTAMP()) OPTIONS (allow_commit_timestamp = true),
			) PRIMARY KEY(T1_I1);`,
			`
			ALTER TABLE T1 ALTER COLUMN T1_T1 DROP DEFAULT;
			ALTER TABLE T1 ALTER COLUMN T1_T1 SET OPTIONS (allow_commit_timestamp = true);
			ALTER TABLE T1 ALTER COLUMN T1_T1 SET DEFAULT (PENDING_COMMIT_TIMESTAMP());`,
			false,
		},
		"disallow commit timestamp and drop pending commit timestamp default value": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_T1 TIMESTAMP DEFAULT (PENDING_COMMIT_TIMESTAMP()) OPTIONS (allow_commit_timestamp = true),
			) PRIMARY KEY(T1_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_T1 TIMESTAMP,
			) PRIMARY KEY(T1_I1);`,
			`
			ALTER TABLE T1 ALTER COLUMN T1_T1 DROP DEFAULT;
			ALTER TABLE T1 ALTER COLUMN T1_T1 SET OPTIONS (allow_commit_timestamp = null);`,
			false,
		},
		"allow commit timestamp and add not null to column with default value": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_T1 TIMESTAMP DEFAULT (CURRENT_TIMESTAMP()),
			) PRIMARY KEY(T1_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_T1 TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
			) PRIMARY KEY(T1_I1);`,
			`
			ALTER TABLE T1 ALTER COLUMN T1_T1 DROP DEFAULT;
			ALTER TABLE T1 ALTER COLUMN T1_T1 SET OPTIONS (allow_commit_timestamp = true);
			ALTER TABLE T1 ALTER COLUMN T1_T1 TIMESTAMP NOT NULL;`,
			false,
		},
		"disallow commit timestamp and add not null with default value": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_T1 TIMESTAMP OPTIONS (allow_commit_timestamp = true),
			) PRIMARY KEY(T1_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_T1 TIMESTAMP NOT NULL DEFAULT (CURRENT_TIMESTAMP()),
			) PRIMARY KEY(T1_I1);`,
			`
			ALTER TABLE T1 ALTER COLUMN T1_T1 SET OPTIONS (allow_commit_timestamp = null);
			ALTER TABLE T1 ALTER COLUMN T1_T1 TIMESTAMP NOT NULL DEFAULT (CURRENT_TIMESTAMP());`,
			false,
		},
		"drop default value and set allow_commit_timestamp to false": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_T1 TIMESTAMP DEFAULT (CURRENT_TIMESTAMP()),
			) PRIMARY KEY(T1_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_T1 TIMESTAMP OPTIONS (allow_commit_timestamp = false),
			) PRIMARY KEY(T1_I1);`,
			`
			ALTER TABLE T1 ALTER COLUMN T1_T1 SET OPTIONS (allow_commit_timestamp = false);
			ALTER TABLE T1 ALTER COLUMN T1_T1 DROP DEFAULT;`,
			false,
		},
		"change default value and add not null": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX) DEFAULT ('a'),
			) PRIMARY KEY(T1_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX) NOT NULL DEFAULT ('b'),
			) PRIMARY KEY(T1_I1);`,
			`
			ALTER TABLE T1 ALTER COLUMN T1_S1 STRING(MAX) NOT NULL DEFAULT ('b');`,
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
		"recreate uuid columns": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_U1 UUID,
			  T1_U2 UUID,
			) PRIMARY KEY(T1_I1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_U1 BYTES(MAX),
			  T1_U2 examples.Message,
			) PRIMARY KEY(T1_I1)`,
			`
			ALTER TABLE T1 DROP COLUMN T1_U2;
			ALTER TABLE T1 DROP COLUMN T1_U1;
			ALTER TABLE T1 ADD COLUMN T1_U1 BYTES(MAX);
			ALTER TABLE T1 ADD COLUMN T1_U2 examples.Message;`,
			false,
		},
		"change element type of array column from string to bytes": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_A1 ARRAY<STRING(MAX)>,
			) PRIMARY KEY(T1_I1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_A1 ARRAY<BYTES(MAX)>,
			) PRIMARY KEY(T1_I1)`,
			`
			ALTER TABLE T1 ALTER COLUMN T1_A1 ARRAY<BYTES(MAX)>;`,
			false,
		},
		"change element type of array column from bytes to string": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_A1 ARRAY<BYTES(MAX)>,
			) PRIMARY KEY(T1_I1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_A1 ARRAY<STRING(MAX)>,
			) PRIMARY KEY(T1_I1)`,
			`
			ALTER TABLE T1 ALTER COLUMN T1_A1 ARRAY<STRING(MAX)>;`,
			false,
		},
		"change element type and length of array column": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_A1 ARRAY<STRING(10)>,
			) PRIMARY KEY(T1_I1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_A1 ARRAY<BYTES(MAX)>,
			) PRIMARY KEY(T1_I1)`,
			`
			ALTER TABLE T1 ALTER COLUMN T1_A1 ARRAY<BYTES(MAX)>;`,
			false,
		},
		"change element type of not null array column with default": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_A1 ARRAY<STRING(MAX)> NOT NULL DEFAULT ([]),
			) PRIMARY KEY(T1_I1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_A1 ARRAY<BYTES(MAX)> NOT NULL DEFAULT ([]),
			) PRIMARY KEY(T1_I1)`,
			`
			ALTER TABLE T1 ALTER COLUMN T1_A1 ARRAY<BYTES(MAX)> NOT NULL DEFAULT ([]);`,
			false,
		},
		"change element type of not null array column": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_A1 ARRAY<STRING(MAX)> NOT NULL,
			) PRIMARY KEY(T1_I1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_A1 ARRAY<BYTES(MAX)> NOT NULL,
			) PRIMARY KEY(T1_I1)`,
			`
			ALTER TABLE T1 ALTER COLUMN T1_A1 ARRAY<BYTES(MAX)> NOT NULL;`,
			false,
		},
		"change element type of array columns between proto and bytes": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_A1 ARRAY<examples.Message>,
			  T1_A2 ARRAY<BYTES(MAX)>,
			) PRIMARY KEY(T1_I1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_A1 ARRAY<BYTES(MAX)>,
			  T1_A2 ARRAY<examples.Message>,
			) PRIMARY KEY(T1_I1)`,
			`
			ALTER TABLE T1 ALTER COLUMN T1_A1 ARRAY<BYTES(MAX)>;
			ALTER TABLE T1 ALTER COLUMN T1_A2 ARRAY<examples.Message>;`,
			false,
		},
		"change element type of array column to int64": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_A1 ARRAY<STRING(MAX)>,
			) PRIMARY KEY(T1_I1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_A1 ARRAY<INT64>,
			) PRIMARY KEY(T1_I1)`,
			`
			ALTER TABLE T1 DROP COLUMN T1_A1;
			ALTER TABLE T1 ADD COLUMN T1_A1 ARRAY<INT64>;`,
			false,
		},
		"recreate generated column": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64,
			  T1_G1 INT64 AS (T1_I2 + 1) STORED,
			) PRIMARY KEY(T1_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64,
			  T1_G1 INT64 AS (T1_I2 + 2) STORED,
			) PRIMARY KEY(T1_I1);`,
			`
			ALTER TABLE T1 DROP COLUMN T1_G1;
			ALTER TABLE T1 ADD COLUMN T1_G1 INT64 AS (T1_I2 + 2) STORED;`,
			false,
		},
		"alter non-stored generated column": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64,
			  T1_G1 INT64 AS (T1_I2 + 1),
			) PRIMARY KEY(T1_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64,
			  T1_G1 INT64 AS (T1_I2 + 2),
			) PRIMARY KEY(T1_I1);`,
			`
			ALTER TABLE T1 ALTER COLUMN T1_G1 INT64 AS (T1_I2 + 2);`,
			false,
		},
		"recreate indexed non-stored generated column": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64,
			  T1_G1 INT64 AS (T1_I2 + 1),
			) PRIMARY KEY(T1_I1);
			CREATE INDEX IDX1 ON T1(T1_G1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64,
			  T1_G1 INT64 AS (T1_I2 + 2),
			) PRIMARY KEY(T1_I1);
			CREATE INDEX IDX1 ON T1(T1_G1);`,
			`
			DROP INDEX IDX1;
			ALTER TABLE T1 DROP COLUMN T1_G1;
			ALTER TABLE T1 ADD COLUMN T1_G1 INT64 AS (T1_I2 + 2);
			CREATE INDEX IDX1 ON T1(T1_G1);`,
			false,
		},
		"recreate column to not null column": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			) PRIMARY KEY(T1_I1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1)`,
			``,
			true,
		},
		"recreate column to generated column": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64,
			  T1_G1 INT64,
			) PRIMARY KEY(T1_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64,
			  T1_G1 INT64 NOT NULL AS (T1_I2 + 1) STORED,
			) PRIMARY KEY(T1_I1);`,
			`
			ALTER TABLE T1 DROP COLUMN T1_G1;
			ALTER TABLE T1 ADD COLUMN T1_G1 INT64 NOT NULL AS (T1_I2 + 1) STORED;`,
			false,
		},
		"remove stored from generated column": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64,
			  T1_G1 INT64 AS (T1_I2 + 1) STORED,
			) PRIMARY KEY(T1_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64,
			  T1_G1 INT64 AS (T1_I2 + 1),
			) PRIMARY KEY(T1_I1);`,
			`
			ALTER TABLE T1 DROP COLUMN T1_G1;
			ALTER TABLE T1 ADD COLUMN T1_G1 INT64 AS (T1_I2 + 1);`,
			false,
		},
		"add stored to generated column": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64,
			  T1_G1 INT64 AS (T1_I2 + 1),
			) PRIMARY KEY(T1_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64,
			  T1_G1 INT64 AS (T1_I2 + 1) STORED,
			) PRIMARY KEY(T1_I1);`,
			`
			ALTER TABLE T1 DROP COLUMN T1_G1;
			ALTER TABLE T1 ADD COLUMN T1_G1 INT64 AS (T1_I2 + 1) STORED;`,
			false,
		},
		"remove stored from indexed generated column": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64,
			  T1_G1 INT64 AS (T1_I2 + 1) STORED,
			) PRIMARY KEY(T1_I1);
			CREATE INDEX IDX1 ON T1(T1_G1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64,
			  T1_G1 INT64 AS (T1_I2 + 1),
			) PRIMARY KEY(T1_I1);
			CREATE INDEX IDX1 ON T1(T1_G1);`,
			`
			DROP INDEX IDX1;
			ALTER TABLE T1 DROP COLUMN T1_G1;
			ALTER TABLE T1 ADD COLUMN T1_G1 INT64 AS (T1_I2 + 1);
			CREATE INDEX IDX1 ON T1(T1_G1);`,
			false,
		},
		"hide generated column": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64,
			  T1_G1 INT64 AS (T1_I2 + 1) STORED,
			) PRIMARY KEY(T1_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64,
			  T1_G1 INT64 AS (T1_I2 + 1) STORED HIDDEN,
			) PRIMARY KEY(T1_I1);`,
			`
			ALTER TABLE T1 DROP COLUMN T1_G1;
			ALTER TABLE T1 ADD COLUMN T1_G1 INT64 AS (T1_I2 + 1) STORED HIDDEN;`,
			false,
		},
		"unhide generated column": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64,
			  T1_G1 INT64 AS (T1_I2 + 1) STORED HIDDEN,
			) PRIMARY KEY(T1_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64,
			  T1_G1 INT64 AS (T1_I2 + 1) STORED,
			) PRIMARY KEY(T1_I1);`,
			`
			ALTER TABLE T1 DROP COLUMN T1_G1;
			ALTER TABLE T1 ADD COLUMN T1_G1 INT64 AS (T1_I2 + 1) STORED;`,
			false,
		},
		"hide column": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			) PRIMARY KEY(T1_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX) HIDDEN,
			) PRIMARY KEY(T1_I1);`,
			``,
			true,
		},
		"unhide column": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX) HIDDEN,
			) PRIMARY KEY(T1_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			) PRIMARY KEY(T1_I1);`,
			``,
			true,
		},
		"keep hidden stored generated column": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64,
			  T1_G1 INT64 AS (T1_I2 + 1) STORED HIDDEN,
			) PRIMARY KEY(T1_I1);`,
			`
			CREATE TABLE T1 (
			  t1_i1 INT64 NOT NULL,
			  T1_I2 INT64,
			  T1_G1 INT64 AS (T1_I2 + 1) STORED HIDDEN,
			) PRIMARY KEY(T1_I1);`,
			``,
			false,
		},
		"add column referenced by new generated column": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			  T1_B1 BOOL AS (T1_S1 IS NOT NULL) STORED,
			) PRIMARY KEY(T1_I1);`,
			`
			ALTER TABLE T1 ADD COLUMN T1_S1 STRING(MAX);
			ALTER TABLE T1 ADD COLUMN T1_B1 BOOL AS (T1_S1 IS NOT NULL) STORED;`,
			false,
		},
		"drop generated column and column it references": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			  T1_B1 BOOL AS (T1_S1 IS NOT NULL) STORED,
			) PRIMARY KEY(T1_I1);
			CREATE INDEX IDX1 ON T1(T1_B1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1);`,
			`
			DROP INDEX IDX1;
			ALTER TABLE T1 DROP COLUMN T1_B1;
			ALTER TABLE T1 DROP COLUMN T1_S1;`,
			false,
		},
		"change type of column referenced by generated column": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			  T1_B1 BOOL AS (T1_S1 IS NOT NULL) STORED,
			) PRIMARY KEY(T1_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 BYTES(MAX),
			  T1_B1 BOOL AS (T1_S1 IS NOT NULL) STORED,
			) PRIMARY KEY(T1_I1);`,
			`
			ALTER TABLE T1 DROP COLUMN T1_B1;
			ALTER TABLE T1 ALTER COLUMN T1_S1 BYTES(MAX);
			ALTER TABLE T1 ADD COLUMN T1_B1 BOOL AS (T1_S1 IS NOT NULL) STORED;`,
			false,
		},
		"recreate column referenced by generated column": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			  T1_B1 BOOL AS (T1_S1 IS NOT NULL) STORED,
			) PRIMARY KEY(T1_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 INT64,
			  T1_B1 BOOL AS (T1_S1 IS NOT NULL) STORED,
			) PRIMARY KEY(T1_I1);`,
			`
			ALTER TABLE T1 DROP COLUMN T1_B1;
			ALTER TABLE T1 DROP COLUMN T1_S1;
			ALTER TABLE T1 ADD COLUMN T1_S1 INT64;
			ALTER TABLE T1 ADD COLUMN T1_B1 BOOL AS (T1_S1 IS NOT NULL) STORED;`,
			false,
		},
		"drop column that non-stored generated column stops referencing": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			  T1_S2 STRING(MAX),
			  T1_B1 BOOL AS (T1_S1 IS NOT NULL),
			) PRIMARY KEY(T1_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S2 STRING(MAX),
			  T1_B1 BOOL AS (T1_S2 IS NOT NULL),
			) PRIMARY KEY(T1_I1);`,
			`
			ALTER TABLE T1 DROP COLUMN T1_B1;
			ALTER TABLE T1 DROP COLUMN T1_S1;
			ALTER TABLE T1 ADD COLUMN T1_B1 BOOL AS (T1_S2 IS NOT NULL);`,
			false,
		},
		"recreate column that non-stored generated column stops referencing": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			  T1_S2 STRING(MAX),
			  T1_B1 BOOL AS (T1_S1 IS NOT NULL),
			) PRIMARY KEY(T1_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 INT64,
			  T1_S2 STRING(MAX),
			  T1_B1 BOOL AS (T1_S2 IS NOT NULL),
			) PRIMARY KEY(T1_I1);`,
			`
			ALTER TABLE T1 DROP COLUMN T1_B1;
			ALTER TABLE T1 DROP COLUMN T1_S1;
			ALTER TABLE T1 ADD COLUMN T1_B1 BOOL AS (T1_S2 IS NOT NULL);
			ALTER TABLE T1 ADD COLUMN T1_S1 INT64;`,
			false,
		},
		"drop column not referenced by generated column": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			  T1_S2 STRING(MAX),
			  T1_B1 BOOL AS (T1_S1 IS NOT NULL) STORED,
			) PRIMARY KEY(T1_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			  T1_B1 BOOL AS (T1_S1 IS NOT NULL) STORED,
			) PRIMARY KEY(T1_I1);`,
			`
			ALTER TABLE T1 DROP COLUMN T1_S2;`,
			false,
		},
		"change length of column referenced by generated column": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(10),
			  T1_B1 BOOL AS (T1_S1 IS NOT NULL) STORED,
			) PRIMARY KEY(T1_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			  T1_B1 BOOL AS (T1_S1 IS NOT NULL) STORED,
			) PRIMARY KEY(T1_I1);`,
			`
			ALTER TABLE T1 ALTER COLUMN T1_S1 STRING(MAX);`,
			false,
		},
		"error on converting column to identity column": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64,
			) PRIMARY KEY(T1_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64 GENERATED BY DEFAULT AS IDENTITY (BIT_REVERSED_POSITIVE),
			) PRIMARY KEY(T1_I1);`,
			``,
			true,
		},
		"error on converting identity column to column": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64 GENERATED BY DEFAULT AS IDENTITY (BIT_REVERSED_POSITIVE),
			) PRIMARY KEY(T1_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64,
			) PRIMARY KEY(T1_I1);`,
			``,
			true,
		},
		"set skip range of identity column": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL GENERATED BY DEFAULT AS IDENTITY (BIT_REVERSED_POSITIVE),
			) PRIMARY KEY(T1_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL GENERATED BY DEFAULT AS IDENTITY (BIT_REVERSED_POSITIVE SKIP RANGE 1, 1000),
			) PRIMARY KEY(T1_I1);`,
			`
			ALTER TABLE T1 ALTER COLUMN T1_I1 ALTER IDENTITY SET SKIP RANGE 1, 1000;`,
			false,
		},
		"change skip range of identity column": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL GENERATED BY DEFAULT AS IDENTITY (BIT_REVERSED_POSITIVE SKIP RANGE 1, 1000),
			) PRIMARY KEY(T1_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL GENERATED BY DEFAULT AS IDENTITY (BIT_REVERSED_POSITIVE SKIP RANGE 1, 2000),
			) PRIMARY KEY(T1_I1);`,
			`
			ALTER TABLE T1 ALTER COLUMN T1_I1 ALTER IDENTITY SET SKIP RANGE 1, 2000;`,
			false,
		},
		"unset skip range of identity column": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL GENERATED BY DEFAULT AS IDENTITY (BIT_REVERSED_POSITIVE SKIP RANGE 1, 1000),
			) PRIMARY KEY(T1_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL GENERATED BY DEFAULT AS IDENTITY (BIT_REVERSED_POSITIVE),
			) PRIMARY KEY(T1_I1);`,
			`
			ALTER TABLE T1 ALTER COLUMN T1_I1 ALTER IDENTITY SET NO SKIP RANGE;`,
			false,
		},
		"error on changing start counter of identity column": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL GENERATED BY DEFAULT AS IDENTITY (BIT_REVERSED_POSITIVE START COUNTER WITH 1000),
			) PRIMARY KEY(T1_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL GENERATED BY DEFAULT AS IDENTITY (BIT_REVERSED_POSITIVE START COUNTER WITH 2000),
			) PRIMARY KEY(T1_I1);`,
			``,
			true,
		},
		"ignore sequence kind of identity column": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL GENERATED BY DEFAULT AS IDENTITY (BIT_REVERSED_POSITIVE),
			) PRIMARY KEY(T1_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL AUTO_INCREMENT,
			) PRIMARY KEY(T1_I1);`,
			``,
			false,
		},
	})
}

func TestDiffColumnZeroDefaultForNotNull(t *testing.T) {
	runDiffTests(t, DiffOption{ErrorOnUnsupportedDDL: true, ZeroDefaultForNotNullColumns: true}, map[string]diffTest{
		"add not null columns": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_BL1 BOOL NOT NULL,
			  T1_I2 INT64 NOT NULL,
			  T1_F1 FLOAT32 NOT NULL,
			  T1_F2 FLOAT64 NOT NULL,
			  T1_N1 NUMERIC NOT NULL,
			  T1_S1 STRING(MAX) NOT NULL,
			  T1_S2 STRING(10) NOT NULL,
			  T1_BY1 BYTES(MAX) NOT NULL,
			  T1_D1 DATE NOT NULL,
			  T1_T1 TIMESTAMP NOT NULL,
			  T1_J1 JSON NOT NULL,
			  T1_AF1 ARRAY<FLOAT64> NOT NULL,
			  T1_U1 UUID NOT NULL,
			  T1_AU1 ARRAY<UUID> NOT NULL,
			) PRIMARY KEY(T1_I1)`,
			`
			ALTER TABLE T1 ADD COLUMN T1_AF1 ARRAY<FLOAT64> NOT NULL DEFAULT (ARRAY[]);
			ALTER TABLE T1 ALTER COLUMN T1_AF1 DROP DEFAULT;
			ALTER TABLE T1 ADD COLUMN T1_AU1 ARRAY<UUID> NOT NULL DEFAULT (ARRAY[]);
			ALTER TABLE T1 ALTER COLUMN T1_AU1 DROP DEFAULT;
			ALTER TABLE T1 ADD COLUMN T1_BL1 BOOL NOT NULL DEFAULT (FALSE);
			ALTER TABLE T1 ALTER COLUMN T1_BL1 DROP DEFAULT;
			ALTER TABLE T1 ADD COLUMN T1_BY1 BYTES(MAX) NOT NULL DEFAULT (b'');
			ALTER TABLE T1 ALTER COLUMN T1_BY1 DROP DEFAULT;
			ALTER TABLE T1 ADD COLUMN T1_D1 DATE NOT NULL DEFAULT (DATE '0001-01-01');
			ALTER TABLE T1 ALTER COLUMN T1_D1 DROP DEFAULT;
			ALTER TABLE T1 ADD COLUMN T1_F1 FLOAT32 NOT NULL DEFAULT (0);
			ALTER TABLE T1 ALTER COLUMN T1_F1 DROP DEFAULT;
			ALTER TABLE T1 ADD COLUMN T1_F2 FLOAT64 NOT NULL DEFAULT (0);
			ALTER TABLE T1 ALTER COLUMN T1_F2 DROP DEFAULT;
			ALTER TABLE T1 ADD COLUMN T1_I2 INT64 NOT NULL DEFAULT (0);
			ALTER TABLE T1 ALTER COLUMN T1_I2 DROP DEFAULT;
			ALTER TABLE T1 ADD COLUMN T1_J1 JSON NOT NULL DEFAULT (JSON '{}');
			ALTER TABLE T1 ALTER COLUMN T1_J1 DROP DEFAULT;
			ALTER TABLE T1 ADD COLUMN T1_N1 NUMERIC NOT NULL DEFAULT (NUMERIC '0');
			ALTER TABLE T1 ALTER COLUMN T1_N1 DROP DEFAULT;
			ALTER TABLE T1 ADD COLUMN T1_S1 STRING(MAX) NOT NULL DEFAULT ('');
			ALTER TABLE T1 ALTER COLUMN T1_S1 DROP DEFAULT;
			ALTER TABLE T1 ADD COLUMN T1_S2 STRING(10) NOT NULL DEFAULT ('');
			ALTER TABLE T1 ALTER COLUMN T1_S2 DROP DEFAULT;
			ALTER TABLE T1 ADD COLUMN T1_T1 TIMESTAMP NOT NULL DEFAULT (TIMESTAMP '0001-01-01T00:00:00Z');
			ALTER TABLE T1 ALTER COLUMN T1_T1 DROP DEFAULT;
			ALTER TABLE T1 ADD COLUMN T1_U1 UUID NOT NULL DEFAULT (CAST('00000000-0000-0000-0000-000000000000' AS UUID));
			ALTER TABLE T1 ALTER COLUMN T1_U1 DROP DEFAULT;`,
			false,
		},
		"add not null commit timestamp column": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_T1 TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
			) PRIMARY KEY(T1_I1)`,
			`
			ALTER TABLE T1 ADD COLUMN T1_T1 TIMESTAMP NOT NULL DEFAULT (TIMESTAMP '0001-01-01T00:00:00Z');
			ALTER TABLE T1 ALTER COLUMN T1_T1 DROP DEFAULT;
			ALTER TABLE T1 ALTER COLUMN T1_T1 SET OPTIONS (allow_commit_timestamp = true);`,
			false,
		},
		"add not null column with default": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX) NOT NULL DEFAULT ('a'),
			) PRIMARY KEY(T1_I1)`,
			`
			ALTER TABLE T1 ADD COLUMN T1_S1 STRING(MAX) NOT NULL DEFAULT ('a');`,
			false,
		},
		"add not null proto column": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_P1 examples.shipping.Order NOT NULL,
			) PRIMARY KEY(T1_I1)`,
			``,
			true,
		},
		"recreate column to not null column": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			) PRIMARY KEY(T1_I1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1)`,
			`
			ALTER TABLE T1 DROP COLUMN T1_S1;
			ALTER TABLE T1 ADD COLUMN T1_S1 INT64 NOT NULL DEFAULT (0);
			ALTER TABLE T1 ALTER COLUMN T1_S1 DROP DEFAULT;`,
			false,
		},
		"change element type of not null array column": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_A1 ARRAY<STRING(MAX)> NOT NULL,
			) PRIMARY KEY(T1_I1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_A1 ARRAY<BYTES(MAX)> NOT NULL,
			) PRIMARY KEY(T1_I1)`,
			`
			ALTER TABLE T1 ALTER COLUMN T1_A1 ARRAY<BYTES(MAX)> NOT NULL;`,
			false,
		},
	})
}
