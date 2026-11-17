package spannerdiff

import "testing"

func TestDiffRole(t *testing.T) {
	runDiffTests(t, DiffOption{ErrorOnUnsupportedDDL: true}, map[string]diffTest{
		"add role": {
			``,
			`
			CREATE ROLE R1;`,
			`CREATE ROLE R1;`,
			false,
		},
		"drop role": {
			`
			CREATE ROLE R1;`,
			``,
			`
			DROP ROLE R1;`,
			false,
		},
	})
}
