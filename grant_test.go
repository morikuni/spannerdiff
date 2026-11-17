package spannerdiff

import "testing"

func TestDiffGrant(t *testing.T) {
	runDiffTests(t, DiffOption{ErrorOnUnsupportedDDL: true}, map[string]diffTest{
		"add table grant": {
			``,
			`
			GRANT SELECT, UPDATE ON TABLE T1 TO ROLE R1;`,
			`
			GRANT SELECT, UPDATE ON TABLE T1 TO ROLE R1;`,
			false,
		},
		"add table grant on recreated table": {
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
			GRANT SELECT ON TABLE T1 TO ROLE R1;`,
			`
			DROP TABLE T1;
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1, T1_I2);
			GRANT SELECT ON TABLE T1 TO ROLE R1;`,
			false,
		},
		"drop table grant": {
			`
			GRANT SELECT, UPDATE ON TABLE T1 TO ROLE R1;`,
			``,
			`
			REVOKE SELECT, UPDATE ON TABLE T1 FROM ROLE R1;`,
			false,
		},
		"alter table grant": {
			`
			GRANT SELECT, SELECT(T1_C1), UPDATE, INSERT(T1_C1, T1_C2) ON TABLE T1 TO ROLE R1;
			GRANT UPDATE, DELETE ON TABLE T1 TO ROLE R2;`,
			`
			GRANT SELECT(T1_C2), DELETE ON TABLE T1 TO ROLE R1;
			GRANT SELECT, UPDATE(T1_C1, T1_C2), UPDATE, INSERT ON TABLE T1 TO ROLE R2;`,
			`
			REVOKE DELETE ON TABLE T1 FROM ROLE R2;
			REVOKE SELECT, SELECT(T1_C1), UPDATE, INSERT(T1_C1, T1_C2) ON TABLE T1 FROM ROLE R1;
			GRANT SELECT(T1_C2), DELETE ON TABLE T1 TO ROLE R1;
			GRANT SELECT, UPDATE(T1_C1, T1_C2), INSERT ON TABLE T1 TO ROLE R2;`,
			false,
		},
		"revoke column privilege before dropping column": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_C1 STRING(MAX),
			) PRIMARY KEY(T1_I1);
			CREATE ROLE R1;
			GRANT SELECT(T1_I1, T1_C1) ON TABLE T1 TO ROLE R1;`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1);
			CREATE ROLE R1;
			GRANT SELECT(T1_I1) ON TABLE T1 TO ROLE R1;`,
			`
			REVOKE SELECT(T1_C1) ON TABLE T1 FROM ROLE R1;
			ALTER TABLE T1 DROP COLUMN T1_C1;`,
			false,
		},
		"add view grant": {
			``,
			`
			GRANT SELECT ON VIEW V1 TO ROLE R1;`,
			`
			GRANT SELECT ON VIEW V1 TO ROLE R1;`,
			false,
		},
		"drop view grant": {
			`
			GRANT SELECT ON VIEW V1 TO ROLE R1;`,
			``,
			`
			REVOKE SELECT ON VIEW V1 FROM ROLE R1;`,
			false,
		},
		"add change stream grant": {
			``,
			`
			GRANT SELECT ON CHANGE STREAM S1 TO ROLE R1;`,
			`
			GRANT SELECT ON CHANGE STREAM S1 TO ROLE R1;`,
			false,
		},
		"drop change stream grant": {
			`
			GRANT SELECT ON CHANGE STREAM S1 TO ROLE R1;`,
			``,
			`
			REVOKE SELECT ON CHANGE STREAM S1 FROM ROLE R1;`,
			false,
		},
		"add table function grant": {
			``,
			`
			GRANT EXECUTE ON TABLE FUNCTION READ_CS1 TO ROLE R1;`,
			`
			GRANT EXECUTE ON TABLE FUNCTION READ_CS1 TO ROLE R1;`,
			false,
		},
		"drop table function grant": {
			`
			GRANT EXECUTE ON TABLE FUNCTION READ_CS1 TO ROLE R1;`,
			``,
			`
			REVOKE EXECUTE ON TABLE FUNCTION READ_CS1 FROM ROLE R1;`,
			false,
		},
		"add role grant": {
			``,
			`
			GRANT ROLE R2 TO ROLE R1;`,
			`
			GRANT ROLE R2 TO ROLE R1;`,
			false,
		},
		"drop role grant": {
			`
			GRANT ROLE R2 TO ROLE R1;`,
			``,
			`
			REVOKE ROLE R2 FROM ROLE R1;`,
			false,
		},
		"add sequence grant": {
			``,
			`
			GRANT SELECT, UPDATE ON SEQUENCE S1, SCH1.S2 TO ROLE R1;`,
			`
			GRANT SELECT, UPDATE ON SEQUENCE S1 TO ROLE R1;
			GRANT SELECT, UPDATE ON SEQUENCE SCH1.S2 TO ROLE R1;`,
			false,
		},
		"alter sequence grant": {
			`
			GRANT SELECT ON SEQUENCE S1 TO ROLE R1;
			GRANT UPDATE ON SEQUENCE S1 TO ROLE R1;`,
			`
			GRANT SELECT ON SEQUENCE S1 TO ROLE R1;`,
			`
			REVOKE UPDATE ON SEQUENCE S1 FROM ROLE R1;`,
			false,
		},
		"revoke sequence grant before dropping sequence": {
			`
			CREATE SEQUENCE S1 OPTIONS (sequence_kind = 'bit_reversed_positive');
			GRANT SELECT ON SEQUENCE S1 TO ROLE R1;`,
			``,
			`
			REVOKE SELECT ON SEQUENCE S1 FROM ROLE R1;
			DROP SEQUENCE S1;`,
			false,
		},
		"add schema usage grant": {
			``,
			`
			GRANT USAGE ON SCHEMA DEFAULT TO ROLE R1;
			GRANT USAGE ON SCHEMA SCH1, DB1.SCH2 TO ROLE R1;`,
			`
			GRANT USAGE ON SCHEMA DEFAULT TO ROLE R1;
			GRANT USAGE ON SCHEMA SCH1 TO ROLE R1;
			GRANT USAGE ON SCHEMA DB1.SCH2 TO ROLE R1;`,
			false,
		},
		"drop schema usage grant": {
			`
			GRANT USAGE ON SCHEMA SCH1 TO ROLE R1;`,
			``,
			`
			REVOKE USAGE ON SCHEMA SCH1 FROM ROLE R1;`,
			false,
		},
		"error on grant on all objects in schema": {
			``,
			`
			GRANT SELECT ON ALL TABLES IN SCHEMA SCH1 TO ROLE R1;`,
			``,
			true,
		},
	})
}
