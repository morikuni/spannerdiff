package spannerdiff

import "testing"

func TestDiffProtoBundle(t *testing.T) {
	runDiffTests(t, DiffOption{ErrorOnUnsupportedDDL: true}, map[string]diffTest{
		"add proto bundle": {
			``,
			"CREATE PROTO BUNDLE (`test.proto`)",
			"CREATE PROTO BUNDLE (`test.proto`)",
			false,
		},
		"drop proto bundle": {
			"CREATE PROTO BUNDLE (`test.proto`)",
			``,
			"DROP PROTO BUNDLE",
			false,
		},
		"alter proto bundle": {
			"CREATE PROTO BUNDLE (`test.proto`)",
			"CREATE PROTO BUNDLE (`test2.proto`)",
			"ALTER PROTO BUNDLE INSERT (`test2.proto`) DELETE (`test.proto`)",
			false,
		},
		"proto bundle twice": {
			"CREATE PROTO BUNDLE (`test.proto`); CREATE PROTO BUNDLE (`test2.proto`)",
			"CREATE PROTO BUNDLE (`test.proto`); CREATE PROTO BUNDLE (`test2.proto`)",
			"",
			true,
		},
	})
}
