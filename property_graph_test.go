package spannerdiff

import "testing"

func TestDiffPropertyGraph(t *testing.T) {
	runDiffTests(t, DiffOption{ErrorOnUnsupportedDDL: true}, map[string]diffTest{
		"add property graph": {
			``,
			`
			CREATE PROPERTY GRAPH G1 NODE TABLES (T1, T2);
			`,
			`
			CREATE PROPERTY GRAPH G1 NODE TABLES (T1, T2);`,
			false,
		},
		"add property graph on recreated table": {
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
			CREATE PROPERTY GRAPH G1 NODE TABLES (T1);`,
			`
			DROP TABLE T1;
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1, T1_I2);
			CREATE PROPERTY GRAPH G1 NODE TABLES (T1);`,
			false,
		},
		"drop property graph": {
			`
			CREATE PROPERTY GRAPH G1 NODE TABLES (T1, T2);`,
			``,
			`
			DROP PROPERTY GRAPH G1;`,
			false,
		},
		"recreate property graph": {
			`
			CREATE PROPERTY GRAPH G1 NODE TABLES (T1, T2);`,
			`
			CREATE PROPERTY GRAPH G1 NODE TABLES (T1);`,
			`
			CREATE OR REPLACE PROPERTY GRAPH G1 NODE TABLES (T1);`,
			false,
		},
		"recreate property graph by recreating edge destination column": {
			`
			CREATE TABLE Person (
			  id INT64 NOT NULL,
			) PRIMARY KEY(id);
			CREATE TABLE Account (
			  id INT64 NOT NULL,
			) PRIMARY KEY(id);
			CREATE TABLE Transfer (
			  id INT64 NOT NULL,
			  person_id INT64 NOT NULL,
			  account_id INT64 NOT NULL,
			) PRIMARY KEY(id);
			CREATE PROPERTY GRAPH G1
			  NODE TABLES (Person, Account)
			  EDGE TABLES (
			    Transfer
			      SOURCE KEY (person_id) REFERENCES Person
			      DESTINATION KEY (account_id) REFERENCES Account (id)
			  );`,
			`
			CREATE TABLE Person (
			  id INT64 NOT NULL,
			) PRIMARY KEY(id);
			CREATE TABLE Account (
			  id INT64 NOT NULL,
			) PRIMARY KEY(id);
			CREATE TABLE Transfer (
			  id INT64 NOT NULL,
			  person_id INT64 NOT NULL,
			  account_id STRING(MAX),
			) PRIMARY KEY(id);
			CREATE PROPERTY GRAPH G1
			  NODE TABLES (Person, Account)
			  EDGE TABLES (
			    Transfer
			      SOURCE KEY (person_id) REFERENCES Person
			      DESTINATION KEY (account_id) REFERENCES Account (id)
			  );`,
			`
			DROP PROPERTY GRAPH G1;
			ALTER TABLE Transfer DROP COLUMN account_id;
			ALTER TABLE Transfer ADD COLUMN account_id STRING(MAX);
			CREATE PROPERTY GRAPH G1
			  NODE TABLES (Person, Account)
			  EDGE TABLES (
			    Transfer
			      SOURCE KEY (person_id) REFERENCES Person
			      DESTINATION KEY (account_id) REFERENCES Account (id)
			  );`,
			false,
		},
	})
}
