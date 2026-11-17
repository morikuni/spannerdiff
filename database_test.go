package spannerdiff

import "testing"

func TestDiffDatabase(t *testing.T) {
	runDiffTests(t, DiffOption{ErrorOnUnsupportedDDL: true}, map[string]diffTest{
		"add alter database": {
			``,
			`
			ALTER DATABASE D1 SET OPTIONS (version_retention_period = '1d');`,
			`
			ALTER DATABASE D1 SET OPTIONS (version_retention_period = '1d');`,
			false,
		},
		"no drop alter database": {
			`
			ALTER DATABASE D1 SET OPTIONS (version_retention_period = '1d');`,
			``,
			``,
			false,
		},
		"alter alter database": {
			`
			ALTER DATABASE D1 SET OPTIONS (version_retention_period = '1d', optimizer_version = 1);`,
			`
			ALTER DATABASE D1 SET OPTIONS (version_retention_period = '2d');`,
			`
			ALTER DATABASE D1 SET OPTIONS (version_retention_period = '2d', optimizer_version = null);`,
			false,
		},
		"duplicated option name without other changes": {
			`
			ALTER DATABASE D1 SET OPTIONS (optimizer_version = 2);`,
			`
			ALTER DATABASE D1 SET OPTIONS (optimizer_version = 1, optimizer_version = 2);`,
			``,
			false,
		},
		"duplicated option name with other changes": {
			`
			ALTER DATABASE D1 SET OPTIONS (optimizer_version = 2);`,
			`
			ALTER DATABASE D1 SET OPTIONS (optimizer_version = 1, optimizer_version = 2, version_retention_period = '7d');`,
			`
			ALTER DATABASE D1 SET OPTIONS (version_retention_period = '7d');`,
			false,
		},
		"alter database with different name": {
			`
			ALTER DATABASE D1 SET OPTIONS (version_retention_period = '1d');`,
			`
			ALTER DATABASE D2 SET OPTIONS (version_retention_period = '2d');`,
			`
			ALTER DATABASE D1 SET OPTIONS (version_retention_period = '2d');`,
			false,
		},
		"ignore database name": {
			`
			ALTER DATABASE D1 SET OPTIONS (version_retention_period = '1d');`,
			`
			ALTER DATABASE D2 SET OPTIONS (version_retention_period = '1d');`,
			``,
			false,
		},
	})
}
