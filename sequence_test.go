package spannerdiff

import "testing"

func TestDiffSequence(t *testing.T) {
	runDiffTests(t, DiffOption{ErrorOnUnsupportedDDL: true}, map[string]diffTest{
		"add sequence": {
			``,
			`
			CREATE SEQUENCE S1 OPTIONS (sequence_kind = 'bit_reversed_positive');`,
			`
			CREATE SEQUENCE S1 OPTIONS (sequence_kind = 'bit_reversed_positive');`,
			false,
		},
		"drop sequence": {
			`
			CREATE SEQUENCE S1 OPTIONS (sequence_kind = 'bit_reversed_positive');`,
			``,
			`
			DROP SEQUENCE S1;`,
			false,
		},
		"alter sequence": {
			`
			CREATE SEQUENCE S1 OPTIONS (skip_range_min = 1000, skip_range_max = 2000);`,
			`
			CREATE SEQUENCE S1 OPTIONS (start_counter_with = 10);`,
			`
			ALTER SEQUENCE S1 SET OPTIONS (start_counter_with = 10, skip_range_min = null, skip_range_max = null);`,
			false,
		},
		"create sequence before column using it": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64 DEFAULT (GET_NEXT_SEQUENCE_VALUE(SEQUENCE U1)),
			) PRIMARY KEY(T1_I1);
			CREATE SEQUENCE U1 OPTIONS (sequence_kind = 'bit_reversed_positive');`,
			`
			CREATE SEQUENCE U1 OPTIONS (sequence_kind = 'bit_reversed_positive');
			ALTER TABLE T1 ADD COLUMN T1_I2 INT64 DEFAULT (GET_NEXT_SEQUENCE_VALUE(SEQUENCE U1));`,
			false,
		},
	})
}
