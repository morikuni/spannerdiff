package spannerdiff

import "testing"

func TestDiffModel(t *testing.T) {
	runDiffTests(t, DiffOption{ErrorOnUnsupportedDDL: true}, map[string]diffTest{
		"add model": {
			``,
			`
			CREATE MODEL M1 INPUT (F1 FLOAT64) OUTPUT (F2 FLOAT64) REMOTE OPTIONS ( endpoint = 'model' );`,
			`
			CREATE MODEL M1 INPUT (F1 FLOAT64) OUTPUT (F2 FLOAT64) REMOTE OPTIONS ( endpoint = 'model' );`,
			false,
		},
		"drop model": {
			`
			CREATE MODEL M1 INPUT (F1 FLOAT64) OUTPUT (F2 FLOAT64) REMOTE OPTIONS ( endpoint = 'model' );`,
			``,
			`
			DROP MODEL M1;`,
			false,
		},
		"alter model": {
			`
			CREATE MODEL M1 INPUT (F1 FLOAT64) OUTPUT (F2 FLOAT64) REMOTE OPTIONS ( endpoint = 'model' );`,
			`
			CREATE MODEL M1 INPUT (F1 FLOAT64) OUTPUT (F2 FLOAT64) REMOTE OPTIONS ( endpoint = 'model2' );`,
			`
			ALTER MODEL M1 SET OPTIONS ( endpoint = 'model2' );`,
			false,
		},
		"recreate model": {
			`
			CREATE MODEL M1 INPUT (F1 FLOAT64) OUTPUT (F2 FLOAT64) REMOTE OPTIONS ( endpoint = 'model' );`,
			`
			CREATE MODEL M1 INPUT (F1 FLOAT64) OUTPUT (F3 FLOAT64) REMOTE OPTIONS ( endpoint = 'model' );`,
			`
			CREATE OR REPLACE MODEL M1 INPUT (F1 FLOAT64) OUTPUT (F3 FLOAT64) REMOTE OPTIONS ( endpoint = 'model' );`,
			false,
		},
	})
}
