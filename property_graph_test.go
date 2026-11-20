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
		"no change of property graphs that Spanner writes with omitted keys, labels and properties": {
			`
			CREATE TABLE P (id INT64 NOT NULL, name STRING(MAX), age INT64) PRIMARY KEY(id);
			CREATE TABLE Q (id INT64 NOT NULL, k2 INT64 NOT NULL, title STRING(MAX)) PRIMARY KEY(id, k2);
			CREATE TABLE E (pid INT64 NOT NULL, qid INT64 NOT NULL, qk2 INT64 NOT NULL, w FLOAT64) PRIMARY KEY(pid, qid, qk2);
			CREATE PROPERTY GRAPH G1 NODE TABLES(P KEY(id) LABEL P PROPERTIES(age, id, name));
			CREATE PROPERTY GRAPH G2 NODE TABLES(P AS N KEY(id) LABEL L1 PROPERTIES(name) LABEL L2 NO PROPERTIES);
			CREATE PROPERTY GRAPH G3 NODE TABLES(P KEY(id) LABEL P PROPERTIES(id, name), Q KEY(id, k2) LABEL Q PROPERTIES(id, k2, title));
			CREATE PROPERTY GRAPH G4 NODE TABLES(P KEY(id) LABEL P PROPERTIES(name, age + 1 AS next_age));
			CREATE PROPERTY GRAPH G5
			  NODE TABLES(P KEY(id) LABEL P PROPERTIES(age, id, name), Q KEY(id, k2) LABEL Q PROPERTIES(id, k2, title))
			  EDGE TABLES(E KEY(pid, qid, qk2) SOURCE KEY(pid) REFERENCES P(id) DESTINATION KEY(qid, qk2) REFERENCES Q(id, k2) LABEL E PROPERTIES(pid, qid, qk2, w));
			CREATE PROPERTY GRAPH G6
			  NODE TABLES(P KEY(id) LABEL P PROPERTIES(age, id, name), Q KEY(id, k2) LABEL Q PROPERTIES(id, k2, title))
			  EDGE TABLES(E AS Likes KEY(pid, qid, qk2) SOURCE KEY(pid) REFERENCES P(id) DESTINATION KEY(qid, qk2) REFERENCES Q(id, k2) LABEL Likes PROPERTIES(pid, qid, qk2, w));`,
			`
			CREATE TABLE P (id INT64 NOT NULL, name STRING(MAX), age INT64) PRIMARY KEY(id);
			CREATE TABLE Q (id INT64 NOT NULL, k2 INT64 NOT NULL, title STRING(MAX)) PRIMARY KEY(id, k2);
			CREATE TABLE E (pid INT64 NOT NULL, qid INT64 NOT NULL, qk2 INT64 NOT NULL, w FLOAT64) PRIMARY KEY(pid, qid, qk2);
			CREATE PROPERTY GRAPH G1 NODE TABLES (P);
			CREATE PROPERTY GRAPH G2 NODE TABLES (P AS N KEY (id) LABEL L1 PROPERTIES (name) LABEL L2 NO PROPERTIES);
			CREATE PROPERTY GRAPH G3 NODE TABLES (P PROPERTIES ALL COLUMNS EXCEPT (age), Q DEFAULT LABEL);
			CREATE PROPERTY GRAPH G4 NODE TABLES (P PROPERTIES (age + 1 AS next_age, name));
			CREATE PROPERTY GRAPH G5 NODE TABLES (P, Q) EDGE TABLES (E SOURCE KEY (pid) REFERENCES P (id) DESTINATION KEY (qid, qk2) REFERENCES Q (id, k2));
			CREATE PROPERTY GRAPH G6 NODE TABLES (P, Q) EDGE TABLES (E AS Likes KEY (pid, qid, qk2) SOURCE KEY (pid) REFERENCES P DESTINATION KEY (qid, qk2) REFERENCES Q LABEL Likes PROPERTIES ALL COLUMNS);`,
			``,
			false,
		},
		"replace property graph that has all columns as properties when column is added": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1);
			CREATE PROPERTY GRAPH G1 NODE TABLES (T1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64,
			) PRIMARY KEY(T1_I1);
			CREATE PROPERTY GRAPH G1 NODE TABLES (T1);`,
			`
			ALTER TABLE T1 ADD COLUMN T1_I2 INT64;
			CREATE OR REPLACE PROPERTY GRAPH G1 NODE TABLES (T1);`,
			false,
		},
		"recreate property graph that has all columns as properties when column is dropped": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64,
			) PRIMARY KEY(T1_I1);
			CREATE PROPERTY GRAPH G1 NODE TABLES (T1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1);
			CREATE PROPERTY GRAPH G1 NODE TABLES (T1);`,
			`
			DROP PROPERTY GRAPH G1;
			ALTER TABLE T1 DROP COLUMN T1_I2;
			CREATE PROPERTY GRAPH G1 NODE TABLES (T1);`,
			false,
		},
		"keep property graph when column that is not a property is dropped": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64,
			) PRIMARY KEY(T1_I1);
			CREATE PROPERTY GRAPH G1 NODE TABLES (T1 PROPERTIES (T1_I1));`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1);
			CREATE PROPERTY GRAPH G1 NODE TABLES (T1 PROPERTIES (T1_I1));`,
			`
			ALTER TABLE T1 DROP COLUMN T1_I2;`,
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
