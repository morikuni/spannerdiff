package spannerdiff

import "testing"

func TestDiffVectorIndex(t *testing.T) {
	runDiffTests(t, DiffOption{ErrorOnUnsupportedDDL: true}, map[string]diffTest{
		"add vector index": {
			``,
			`
			CREATE VECTOR INDEX IDX1 ON T1(T1_AF1) OPTIONS (distance_type = 'COSINE');`,
			`
			CREATE VECTOR INDEX IDX1 ON T1(T1_AF1) OPTIONS (distance_type = 'COSINE');`,
			false,
		},
		"add vector index on recreated table": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64 NOT NULL,
			  T1_AF1 ARRAY<FLOAT64> NOT NULL,
			) PRIMARY KEY(T1_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64 NOT NULL,
			  T1_AF1 ARRAY<FLOAT64> NOT NULL,
			) PRIMARY KEY(T1_I1, T1_I2);
			CREATE VECTOR INDEX IDX1 ON T1(T1_AF1) OPTIONS (distance_type = 'COSINE');`,
			`
			DROP TABLE T1;
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64 NOT NULL,
			  T1_AF1 ARRAY<FLOAT64> NOT NULL,
			) PRIMARY KEY(T1_I1, T1_I2);
			CREATE VECTOR INDEX IDX1 ON T1(T1_AF1) OPTIONS (distance_type = 'COSINE');`,
			false,
		},
		"drop vector index": {
			`
			CREATE VECTOR INDEX IDX1 ON T1(T1_AF1) OPTIONS (distance_type = 'COSINE');`,
			``,
			`
			DROP VECTOR INDEX IDX1;`,
			false,
		},
		"recreate vector index": {
			`
			CREATE VECTOR INDEX IDX1 ON T1(T1_AF1) OPTIONS (distance_type = 'COSINE');`,
			`
			CREATE VECTOR INDEX IDX1 ON T1(T1_AF1) OPTIONS (distance_type = 'EUCLIDEAN');`,
			`
			DROP VECTOR INDEX IDX1;
			CREATE VECTOR INDEX IDX1 ON T1(T1_AF1) OPTIONS (distance_type = 'EUCLIDEAN');`,
			false,
		},
		"set disable_search of vector index": {
			`
			CREATE VECTOR INDEX IDX1 ON T1(T1_AF1) OPTIONS (distance_type = 'COSINE');`,
			`
			CREATE VECTOR INDEX IDX1 ON T1(T1_AF1) OPTIONS (distance_type = 'COSINE', disable_search = true);`,
			`
			ALTER VECTOR INDEX IDX1 SET OPTIONS (disable_search = true);`,
			false,
		},
		"alter vector index storing": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_AF1 ARRAY<FLOAT32>(vector_length=>128),
			  T1_S1 STRING(MAX),
			  T1_S2 STRING(MAX),
			) PRIMARY KEY(T1_I1);
			CREATE VECTOR INDEX IDX1 ON T1(T1_AF1) STORING (T1_S1) OPTIONS (distance_type = 'COSINE');`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_AF1 ARRAY<FLOAT32>(vector_length=>128),
			  T1_S2 STRING(MAX),
			) PRIMARY KEY(T1_I1);
			CREATE VECTOR INDEX IDX1 ON T1(T1_AF1) STORING (T1_S2) OPTIONS (distance_type = 'COSINE');`,
			`
			ALTER VECTOR INDEX IDX1 DROP STORED COLUMN T1_S1;
			ALTER TABLE T1 DROP COLUMN T1_S1;
			ALTER VECTOR INDEX IDX1 ADD STORED COLUMN T1_S2;`,
			false,
		},
	})
}
