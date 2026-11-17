package spannerdiff

import "testing"

func TestDiffSchema(t *testing.T) {
	runDiffTests(t, DiffOption{ErrorOnUnsupportedDDL: true}, map[string]diffTest{
		"add schema": {
			``,
			`
			CREATE SCHEMA S1;`,
			`
			CREATE SCHEMA S1;`,
			false,
		},
		"drop schema": {
			`
			CREATE SCHEMA S1;`,
			``,
			`
			DROP SCHEMA S1;`,
			false,
		},
	})
}
