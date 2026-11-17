package spannerdiff

import "testing"

func TestDiffChangeStream(t *testing.T) {
	runDiffTests(t, DiffOption{ErrorOnUnsupportedDDL: true}, map[string]diffTest{
		"add change stream": {
			``,
			`
			CREATE CHANGE STREAM S1 FOR ALL;`,
			`
			CREATE CHANGE STREAM S1 FOR ALL;`,
			false,
		},
		"add change stream for recreated table": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1, T1_I2);
			CREATE CHANGE STREAM S1 FOR T1;`,
			`
			DROP TABLE T1;
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1, T1_I2);
			CREATE CHANGE STREAM S1 FOR T1;`,
			false,
		},
		"drop change stream": {
			`
			CREATE CHANGE STREAM S1 FOR ALL;`,
			``,
			`
			DROP CHANGE STREAM S1;`,
			false,
		},
		"alter change stream": {
			`
			CREATE CHANGE STREAM S1 FOR ALL OPTIONS ( retention_period = '36h' );`,
			`
			CREATE CHANGE STREAM S1 FOR T1(T1_I1) OPTIONS ( retention_period = '72h' );`,
			`
			ALTER CHANGE STREAM S1 SET FOR T1(T1_I1);
			ALTER CHANGE STREAM S1 SET OPTIONS ( retention_period = '72h' );`,
			false,
		},
		"remove table from change stream before dropping table": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1);
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			) PRIMARY KEY(T2_I1);
			CREATE CHANGE STREAM CS1 FOR T1, T2;`,
			`
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			) PRIMARY KEY(T2_I1);
			CREATE TABLE T3 (
			  T3_I1 INT64 NOT NULL,
			) PRIMARY KEY(T3_I1);
			CREATE CHANGE STREAM CS1 FOR T2, T3;`,
			`
			ALTER CHANGE STREAM CS1 SET FOR T2;
			DROP TABLE T1;
			CREATE TABLE T3 (
			  T3_I1 INT64 NOT NULL,
			) PRIMARY KEY(T3_I1);
			ALTER CHANGE STREAM CS1 SET FOR T2, T3;`,
			false,
		},
		"drop change stream for": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1);
			CREATE CHANGE STREAM CS1 FOR T1;`,
			``,
			`
			DROP CHANGE STREAM CS1;
			DROP TABLE T1;`,
			false,
		},
		"remove all tables from change stream": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1);
			CREATE CHANGE STREAM CS1 FOR T1;`,
			`
			CREATE CHANGE STREAM CS1;`,
			`
			ALTER CHANGE STREAM CS1 DROP FOR ALL;
			DROP TABLE T1;`,
			false,
		},
		"alter change stream options": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1);
			CREATE CHANGE STREAM CS1 FOR T1 OPTIONS ( retention_period = '36h' );`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1);
			CREATE CHANGE STREAM CS1 FOR T1 OPTIONS ( retention_period = '72h' );`,
			`
			ALTER CHANGE STREAM CS1 SET OPTIONS ( retention_period = '72h' );`,
			false,
		},
		"remove dropped table from change stream when recreating another watched table": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1);
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			) PRIMARY KEY(T2_I1);
			CREATE CHANGE STREAM CS1 FOR T1, T2;`,
			`
			CREATE TABLE T1 (
			  T1_I1 STRING(MAX) NOT NULL,
			) PRIMARY KEY(T1_I1);
			CREATE CHANGE STREAM CS1 FOR T1;`,
			`
			ALTER CHANGE STREAM CS1 DROP FOR ALL;
			DROP TABLE T2;
			DROP TABLE T1;
			CREATE TABLE T1 (
			  T1_I1 STRING(MAX) NOT NULL,
			) PRIMARY KEY(T1_I1);
			ALTER CHANGE STREAM CS1 SET FOR T1;`,
			false,
		},
		"remove dropped column from change stream when recreating another watched column": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64,
			  T1_I3 INT64,
			) PRIMARY KEY(T1_I1);
			CREATE CHANGE STREAM CS1 FOR T1(T1_I2, T1_I3);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 STRING(MAX),
			) PRIMARY KEY(T1_I1);
			CREATE CHANGE STREAM CS1 FOR T1(T1_I2);`,
			`
			ALTER CHANGE STREAM CS1 DROP FOR ALL;
			ALTER TABLE T1 DROP COLUMN T1_I3;
			ALTER TABLE T1 DROP COLUMN T1_I2;
			ALTER TABLE T1 ADD COLUMN T1_I2 STRING(MAX);
			ALTER CHANGE STREAM CS1 SET FOR T1(T1_I2);`,
			false,
		},
		"set change stream for all before dropping watched table": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1);
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			) PRIMARY KEY(T2_I1);
			CREATE CHANGE STREAM CS1 FOR T2;`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1);
			CREATE CHANGE STREAM CS1 FOR ALL;`,
			`
			ALTER CHANGE STREAM CS1 SET FOR ALL;
			DROP TABLE T2;`,
			false,
		},
		"alter change stream options when recreating watched table": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1);
			CREATE CHANGE STREAM CS1 FOR T1 OPTIONS ( retention_period = '36h' );`,
			`
			CREATE TABLE T1 (
			  T1_I1 STRING(MAX) NOT NULL,
			) PRIMARY KEY(T1_I1);
			CREATE CHANGE STREAM CS1 FOR T1 OPTIONS ( retention_period = '72h' );`,
			`
			ALTER CHANGE STREAM CS1 DROP FOR ALL;
			DROP TABLE T1;
			CREATE TABLE T1 (
			  T1_I1 STRING(MAX) NOT NULL,
			) PRIMARY KEY(T1_I1);
			ALTER CHANGE STREAM CS1 SET FOR T1;
			ALTER CHANGE STREAM CS1 SET OPTIONS ( retention_period = '72h' );`,
			false,
		},
		"track only primary key columns in change stream": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64,
			) PRIMARY KEY(T1_I1);
			CREATE CHANGE STREAM CS1 FOR T1;`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64,
			) PRIMARY KEY(T1_I1);
			CREATE CHANGE STREAM CS1 FOR T1();`,
			`
			ALTER CHANGE STREAM CS1 SET FOR T1();`,
			false,
		},
		"track all columns instead of only primary key columns in change stream": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64,
			) PRIMARY KEY(T1_I1);
			CREATE CHANGE STREAM CS1 FOR T1();`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64,
			) PRIMARY KEY(T1_I1);
			CREATE CHANGE STREAM CS1 FOR T1;`,
			`
			ALTER CHANGE STREAM CS1 SET FOR T1;`,
			false,
		},
		"track only primary key columns instead of dropped column in change stream": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64,
			) PRIMARY KEY(T1_I1);
			CREATE CHANGE STREAM CS1 FOR T1(T1_I2);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1);
			CREATE CHANGE STREAM CS1 FOR T1();`,
			`
			ALTER CHANGE STREAM CS1 SET FOR T1();
			ALTER TABLE T1 DROP COLUMN T1_I2;`,
			false,
		},
		"keep tracking only primary key columns in change stream": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64,
			) PRIMARY KEY(T1_I1);
			CREATE CHANGE STREAM CS1 FOR T1();`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64,
			) PRIMARY KEY(T1_I1);
			CREATE CHANGE STREAM CS1 FOR T1();`,
			``,
			false,
		},
	})
}
