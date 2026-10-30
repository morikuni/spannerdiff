package spannerdiff

import (
	"bytes"
	"strings"
	"testing"

	"github.com/cloudspannerecosystem/memefish"
	"github.com/google/go-cmp/cmp"
)

func TestDiff(t *testing.T) {
	for name, tt := range map[string]struct {
		base      string
		target    string
		wantDDLs  string
		wantError bool
	}{
		"unsupported ddl": {
			``,
			`
			ALTER INDEX IDX1 ADD STORED COLUMN T1_I1;`,
			``,
			true,
		},
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
		"add table": {
			``,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1);`,
			false,
		},
		"drop table": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1)`,
			``,
			`
			DROP TABLE T1;`,
			false,
		},
		"primary key in column definition": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64,
			) PRIMARY KEY(T1_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL PRIMARY KEY,
			  T1_I2 INT64,
			);`,
			``,
			false,
		},
		"primary key as table constraint": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1, T1_I2 DESC);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64 NOT NULL,
			  PRIMARY KEY (T1_I1, T1_I2 DESC),
			);`,
			``,
			false,
		},
		"recreate table by changing primary key as table constraint": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64 NOT NULL,
			  PRIMARY KEY (T1_I1, T1_I2),
			);`,
			`
			DROP TABLE T1;
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64 NOT NULL,
			  PRIMARY KEY (T1_I1, T1_I2),
			);`,
			false,
		},
		"recreate table": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1, T1_S1)`,
			`
			DROP TABLE T1;
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1, T1_S1);`,
			false,
		},
		"add foreign key": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX)
			) PRIMARY KEY(T1_I1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			  CONSTRAINT FK1 FOREIGN KEY (T1_S1) REFERENCES T2 (T2_S1),
			) PRIMARY KEY(T1_I1);
			`,
			`
			ALTER TABLE T1 ADD CONSTRAINT FK1 FOREIGN KEY (T1_S1) REFERENCES T2(T2_S1);`,
			false,
		},
		"add foreign key referencing recreated table": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64,
			) PRIMARY KEY(T1_I1);
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			  T2_I2 INT64,
			) PRIMARY KEY(T2_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64,
			  CONSTRAINT FK1 FOREIGN KEY (T1_I2) REFERENCES T2 (T2_I1),
			) PRIMARY KEY(T1_I1);
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			  T2_I2 INT64,
			) PRIMARY KEY(T2_I1, T2_I2);`,
			`
			DROP TABLE T2;
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			  T2_I2 INT64
			) PRIMARY KEY (T2_I1, T2_I2);
			ALTER TABLE T1 ADD CONSTRAINT FK1 FOREIGN KEY (T1_I2) REFERENCES T2(T2_I1);`,
			false,
		},
		"drop foreign key": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			  CONSTRAINT FK1 FOREIGN KEY (T1_S1) REFERENCES T2 (T2_S1),
			) PRIMARY KEY(T1_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX)
			) PRIMARY KEY(T1_I1);`,
			`
			ALTER TABLE T1 DROP CONSTRAINT FK1;`,
			false,
		},
		"recreate foreign key": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			  CONSTRAINT FK1 FOREIGN KEY (T1_I2) REFERENCES T2 (T2_I1),
			) PRIMARY KEY(T1_I1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			  CONSTRAINT FK1 FOREIGN KEY (T1_S1) REFERENCES T2 (T2_S1),
			) PRIMARY KEY(T1_I1)`,
			`
			ALTER TABLE T1 DROP CONSTRAINT FK1;
			ALTER TABLE T1 ADD CONSTRAINT FK1 FOREIGN KEY (T1_S1) REFERENCES T2(T2_S1);`,
			false,
		},
		"match foreign key without on delete with on delete no action": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			  CONSTRAINT FK1 FOREIGN KEY (T1_S1) REFERENCES T2 (T2_S1),
			) PRIMARY KEY(T1_I1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			  CONSTRAINT FK1 FOREIGN KEY (T1_S1) REFERENCES T2 (T2_S1) ON DELETE NO ACTION,
			) PRIMARY KEY(T1_I1)`,
			``,
			false,
		},
		"match unnamed foreign key with on delete no action with one without on delete": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			  FOREIGN KEY (T1_S1) REFERENCES T2 (T2_S1) ON DELETE NO ACTION,
			) PRIMARY KEY(T1_I1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			  FOREIGN KEY (T1_S1) REFERENCES T2 (T2_S1),
			) PRIMARY KEY(T1_I1)`,
			``,
			false,
		},
		"recreate foreign key by changing on delete from omitted to cascade": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			  CONSTRAINT FK1 FOREIGN KEY (T1_S1) REFERENCES T2 (T2_S1),
			) PRIMARY KEY(T1_I1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			  CONSTRAINT FK1 FOREIGN KEY (T1_S1) REFERENCES T2 (T2_S1) ON DELETE CASCADE,
			) PRIMARY KEY(T1_I1)`,
			`
			ALTER TABLE T1 DROP CONSTRAINT FK1;
			ALTER TABLE T1 ADD CONSTRAINT FK1 FOREIGN KEY (T1_S1) REFERENCES T2(T2_S1) ON DELETE CASCADE;`,
			false,
		},
		"recreate foreign key by changing on delete from no action to cascade": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			  CONSTRAINT FK1 FOREIGN KEY (T1_S1) REFERENCES T2 (T2_S1) ON DELETE NO ACTION,
			) PRIMARY KEY(T1_I1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			  CONSTRAINT FK1 FOREIGN KEY (T1_S1) REFERENCES T2 (T2_S1) ON DELETE CASCADE,
			) PRIMARY KEY(T1_I1)`,
			`
			ALTER TABLE T1 DROP CONSTRAINT FK1;
			ALTER TABLE T1 ADD CONSTRAINT FK1 FOREIGN KEY (T1_S1) REFERENCES T2(T2_S1) ON DELETE CASCADE;`,
			false,
		},
		"recreate foreign key by changing on delete from cascade to omitted": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			  CONSTRAINT FK1 FOREIGN KEY (T1_S1) REFERENCES T2 (T2_S1) ON DELETE CASCADE,
			) PRIMARY KEY(T1_I1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			  CONSTRAINT FK1 FOREIGN KEY (T1_S1) REFERENCES T2 (T2_S1),
			) PRIMARY KEY(T1_I1)`,
			`
			ALTER TABLE T1 DROP CONSTRAINT FK1;
			ALTER TABLE T1 ADD CONSTRAINT FK1 FOREIGN KEY (T1_S1) REFERENCES T2(T2_S1);`,
			false,
		},
		"match named foreign key without on delete with unnamed one with on delete no action": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			  CONSTRAINT FK1 FOREIGN KEY (T1_S1) REFERENCES T2 (T2_S1),
			) PRIMARY KEY(T1_I1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			  FOREIGN KEY (T1_S1) REFERENCES T2 (T2_S1) ON DELETE NO ACTION,
			) PRIMARY KEY(T1_I1)`,
			``,
			false,
		},
		"error on changing on delete of unnamed foreign key from omitted to cascade": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			  FOREIGN KEY (T1_S1) REFERENCES T2 (T2_S1),
			) PRIMARY KEY(T1_I1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			  FOREIGN KEY (T1_S1) REFERENCES T2 (T2_S1) ON DELETE CASCADE,
			) PRIMARY KEY(T1_I1)`,
			``,
			true,
		},
		"match unnamed foreign key without on delete with one with on delete no action": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			  FOREIGN KEY (T1_S1) REFERENCES T2 (T2_S1),
			) PRIMARY KEY(T1_I1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			  FOREIGN KEY (T1_S1) REFERENCES T2 (T2_S1) ON DELETE NO ACTION,
			) PRIMARY KEY(T1_I1)`,
			``,
			false,
		},
		"match named foreign key with on delete no action with unnamed one without on delete": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			  CONSTRAINT FK1 FOREIGN KEY (T1_S1) REFERENCES T2 (T2_S1) ON DELETE NO ACTION,
			) PRIMARY KEY(T1_I1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			  FOREIGN KEY (T1_S1) REFERENCES T2 (T2_S1),
			) PRIMARY KEY(T1_I1)`,
			``,
			false,
		},
		"match foreign key with on delete no action with one without on delete": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			  CONSTRAINT FK1 FOREIGN KEY (T1_S1) REFERENCES T2 (T2_S1) ON DELETE NO ACTION,
			) PRIMARY KEY(T1_I1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			  CONSTRAINT FK1 FOREIGN KEY (T1_S1) REFERENCES T2 (T2_S1),
			) PRIMARY KEY(T1_I1)`,
			``,
			false,
		},
		"match foreign keys on same columns by on delete action": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			  CONSTRAINT FK1 FOREIGN KEY (T1_S1) REFERENCES T2 (T2_S1),
			  CONSTRAINT FK2 FOREIGN KEY (T1_S1) REFERENCES T2 (T2_S1) ON DELETE CASCADE,
			) PRIMARY KEY(T1_I1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			  FOREIGN KEY (T1_S1) REFERENCES T2 (T2_S1) ON DELETE CASCADE,
			  FOREIGN KEY (T1_S1) REFERENCES T2 (T2_S1) ON DELETE NO ACTION,
			) PRIMARY KEY(T1_I1)`,
			``,
			false,
		},
		"match not enforced foreign key without on delete with on delete no action": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			  T1_S2 STRING(MAX),
			  CONSTRAINT FK1 FOREIGN KEY (T1_S1) REFERENCES T2 (T2_S1) NOT ENFORCED,
			  FOREIGN KEY (T1_S2) REFERENCES T2 (T2_S1) NOT ENFORCED,
			) PRIMARY KEY(T1_I1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			  T1_S2 STRING(MAX),
			  CONSTRAINT FK1 FOREIGN KEY (T1_S1) REFERENCES T2 (T2_S1) ON DELETE NO ACTION NOT ENFORCED,
			  FOREIGN KEY (T1_S2) REFERENCES T2 (T2_S1) ON DELETE NO ACTION NOT ENFORCED,
			) PRIMARY KEY(T1_I1)`,
			``,
			false,
		},
		"change enforcement of foreign key from omitted to enforced": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			  CONSTRAINT FK1 FOREIGN KEY (T1_S1) REFERENCES T2 (T2_S1),
			) PRIMARY KEY(T1_I1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			  CONSTRAINT FK1 FOREIGN KEY (T1_S1) REFERENCES T2 (T2_S1) ENFORCED,
			) PRIMARY KEY(T1_I1)`,
			``,
			false,
		},
		"change enforcement of unnamed foreign key from enforced to omitted": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			  FOREIGN KEY (T1_S1) REFERENCES T2 (T2_S1) ENFORCED,
			) PRIMARY KEY(T1_I1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			  FOREIGN KEY (T1_S1) REFERENCES T2 (T2_S1),
			) PRIMARY KEY(T1_I1)`,
			``,
			false,
		},
		"recreate foreign key by changing enforcement from omitted to not enforced": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			  CONSTRAINT FK1 FOREIGN KEY (T1_S1) REFERENCES T2 (T2_S1),
			) PRIMARY KEY(T1_I1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			  CONSTRAINT FK1 FOREIGN KEY (T1_S1) REFERENCES T2 (T2_S1) NOT ENFORCED,
			) PRIMARY KEY(T1_I1)`,
			`
			ALTER TABLE T1 DROP CONSTRAINT FK1;
			ALTER TABLE T1 ADD CONSTRAINT FK1 FOREIGN KEY (T1_S1) REFERENCES T2(T2_S1) NOT ENFORCED;`,
			false,
		},
		"recreate foreign key by changing enforcement from not enforced to omitted": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			  CONSTRAINT FK1 FOREIGN KEY (T1_S1) REFERENCES T2 (T2_S1) NOT ENFORCED,
			) PRIMARY KEY(T1_I1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			  CONSTRAINT FK1 FOREIGN KEY (T1_S1) REFERENCES T2 (T2_S1),
			) PRIMARY KEY(T1_I1)`,
			`
			ALTER TABLE T1 DROP CONSTRAINT FK1;
			ALTER TABLE T1 ADD CONSTRAINT FK1 FOREIGN KEY (T1_S1) REFERENCES T2(T2_S1);`,
			false,
		},
		"error on changing enforcement of unnamed foreign key": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			  FOREIGN KEY (T1_S1) REFERENCES T2 (T2_S1),
			) PRIMARY KEY(T1_I1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			  FOREIGN KEY (T1_S1) REFERENCES T2 (T2_S1) NOT ENFORCED,
			) PRIMARY KEY(T1_I1)`,
			``,
			true,
		},
		"change named foreign key without enforcement to unnamed enforced one": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			  CONSTRAINT FK1 FOREIGN KEY (T1_S1) REFERENCES T2 (T2_S1),
			) PRIMARY KEY(T1_I1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			  FOREIGN KEY (T1_S1) REFERENCES T2 (T2_S1) ENFORCED,
			) PRIMARY KEY(T1_I1)`,
			``,
			false,
		},
		"change named enforced foreign key to unnamed one without enforcement": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			  CONSTRAINT FK1 FOREIGN KEY (T1_S1) REFERENCES T2 (T2_S1) ENFORCED,
			) PRIMARY KEY(T1_I1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			  FOREIGN KEY (T1_S1) REFERENCES T2 (T2_S1),
			) PRIMARY KEY(T1_I1)`,
			``,
			false,
		},
		"change enforcement of unnamed foreign keys on same columns from omitted to enforced": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			  FOREIGN KEY (T1_S1) REFERENCES T2 (T2_S1),
			  FOREIGN KEY (T1_S1) REFERENCES T2 (T2_S1) NOT ENFORCED,
			) PRIMARY KEY(T1_I1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			  FOREIGN KEY (T1_S1) REFERENCES T2 (T2_S1) NOT ENFORCED,
			  FOREIGN KEY (T1_S1) REFERENCES T2 (T2_S1) ENFORCED,
			) PRIMARY KEY(T1_I1)`,
			``,
			false,
		},
		"add foreign key after creating referenced table": {
			``,
			`
			CREATE TABLE B1 (
			  B1_I1 INT64 NOT NULL,
			) PRIMARY KEY(B1_I1);
			CREATE TABLE A1 (
			  A1_I1 INT64 NOT NULL,
			  B1_I1 INT64,
			  CONSTRAINT FK1 FOREIGN KEY (B1_I1) REFERENCES B1 (B1_I1),
			) PRIMARY KEY(A1_I1);`,
			`
			CREATE TABLE A1 (
			  A1_I1 INT64 NOT NULL,
			  B1_I1 INT64,
			) PRIMARY KEY(A1_I1);
			CREATE TABLE B1 (
			  B1_I1 INT64 NOT NULL,
			) PRIMARY KEY(B1_I1);
			ALTER TABLE A1 ADD CONSTRAINT FK1 FOREIGN KEY (B1_I1) REFERENCES B1 (B1_I1);`,
			false,
		},
		"drop foreign key before dropping referenced table": {
			`
			CREATE TABLE B1 (
			  B1_I1 INT64 NOT NULL,
			) PRIMARY KEY(B1_I1);
			CREATE TABLE A1 (
			  A1_I1 INT64 NOT NULL,
			  B1_I1 INT64,
			  CONSTRAINT FK1 FOREIGN KEY (B1_I1) REFERENCES B1 (B1_I1),
			) PRIMARY KEY(A1_I1);`,
			``,
			`
			ALTER TABLE A1 DROP CONSTRAINT FK1;
			DROP TABLE B1;
			DROP TABLE A1;`,
			false,
		},
		"recreate foreign key by recreating referenced table": {
			`
			CREATE TABLE B1 (
			  B1_I1 INT64 NOT NULL,
			) PRIMARY KEY(B1_I1);
			CREATE TABLE A1 (
			  A1_I1 INT64 NOT NULL,
			  B1_I1 INT64,
			  CONSTRAINT FK1 FOREIGN KEY (B1_I1) REFERENCES B1 (B1_I1),
			) PRIMARY KEY(A1_I1);`,
			`
			CREATE TABLE B1 (
			  B1_I1 INT64 NOT NULL,
			  B1_I2 INT64 NOT NULL,
			) PRIMARY KEY(B1_I1, B1_I2);
			CREATE TABLE A1 (
			  A1_I1 INT64 NOT NULL,
			  B1_I1 INT64,
			  CONSTRAINT FK1 FOREIGN KEY (B1_I1) REFERENCES B1 (B1_I1),
			) PRIMARY KEY(A1_I1);`,
			`
			ALTER TABLE A1 DROP CONSTRAINT FK1;
			DROP TABLE B1;
			CREATE TABLE B1 (
			  B1_I1 INT64 NOT NULL,
			  B1_I2 INT64 NOT NULL,
			) PRIMARY KEY(B1_I1, B1_I2);
			ALTER TABLE A1 ADD CONSTRAINT FK1 FOREIGN KEY (B1_I1) REFERENCES B1 (B1_I1);`,
			false,
		},
		"match unnamed constraint with named constraint": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  CONSTRAINT CK_GENERATED CHECK (T1_I1 > 0),
			) PRIMARY KEY(T1_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  CHECK (T1_I1 > 0),
			) PRIMARY KEY(T1_I1);`,
			``,
			false,
		},
		"add unnamed constraint": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  CHECK (T1_I1 > 0),
			) PRIMARY KEY(T1_I1);`,
			`
			ALTER TABLE T1 ADD CHECK (T1_I1 > 0);`,
			false,
		},
		"error on dropping unnamed constraint": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  CHECK (T1_I1 > 0),
			) PRIMARY KEY(T1_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1);`,
			``,
			true,
		},
		"drop check constraint before column": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64,
			  CONSTRAINT C1 CHECK (T1_I2 > 0),
			) PRIMARY KEY(T1_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1);`,
			`
			ALTER TABLE T1 DROP CONSTRAINT C1;
			ALTER TABLE T1 DROP COLUMN T1_I2;`,
			false,
		},
		"add check constraint": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  CONSTRAINT CHK1 CHECK (T1_I1 > 0)
			) PRIMARY KEY(T1_I1)`,
			`
			ALTER TABLE T1 ADD CONSTRAINT CHK1 CHECK (T1_I1 > 0);`,
			false,
		},
		"add check constraint on recreated column": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64,
			) PRIMARY KEY(T1_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 STRING(MAX),
			  CONSTRAINT C1 CHECK (T1_I2 != ''),
			) PRIMARY KEY(T1_I1);`,
			`
			ALTER TABLE T1 DROP COLUMN T1_I2;
			ALTER TABLE T1 ADD COLUMN T1_I2 STRING(MAX);
			ALTER TABLE T1 ADD CONSTRAINT C1 CHECK (T1_I2 != "");`,
			false,
		},
		"drop check constraint": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  CONSTRAINT CHK1 CHECK (T1_I1 > 0)
			) PRIMARY KEY(T1_I1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1)`,
			`
			ALTER TABLE T1 DROP CONSTRAINT CHK1;`,
			false,
		},
		"recreate check constraint": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  CONSTRAINT CHK1 CHECK (T1_I1 > 0)
			) PRIMARY KEY(T1_I1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  CONSTRAINT CHK1 CHECK (T1_I1 > 1)
			) PRIMARY KEY(T1_I1)`,
			`
			ALTER TABLE T1 DROP CONSTRAINT CHK1;
			ALTER TABLE T1 ADD CONSTRAINT CHK1 CHECK (T1_I1 > 1);`,
			false,
		},
		"add row deletion policy": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_TS1 TIMESTAMP NOT NULL,
			) PRIMARY KEY(T1_I1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_TS1 TIMESTAMP NOT NULL,
			) PRIMARY KEY(T1_I1), ROW DELETION POLICY (OLDER_THAN(T1_TS1, INTERVAL 1 DAY));`,
			`
			ALTER TABLE T1 ADD ROW DELETION POLICY (OLDER_THAN(T1_TS1, INTERVAL 1 DAY));`,
			false,
		},
		"add row deletion policy on recreated column": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_TS1 STRING(MAX),
			) PRIMARY KEY(T1_I1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_TS1 TIMESTAMP,
			) PRIMARY KEY(T1_I1), ROW DELETION POLICY (OLDER_THAN(T1_TS1, INTERVAL 1 DAY));`,
			`
			ALTER TABLE T1 DROP COLUMN T1_TS1;
			ALTER TABLE T1 ADD COLUMN T1_TS1 TIMESTAMP;
			ALTER TABLE T1 ADD ROW DELETION POLICY (OLDER_THAN(T1_TS1, INTERVAL 1 DAY));`,
			false,
		},
		"drop row deletion policy": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_TS1 TIMESTAMP NOT NULL,
			) PRIMARY KEY(T1_I1), ROW DELETION POLICY (OLDER_THAN(T1_TS1, INTERVAL 1 DAY));`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_TS1 TIMESTAMP NOT NULL,
			) PRIMARY KEY(T1_I1)`,
			`
			ALTER TABLE T1 DROP ROW DELETION POLICY;`,
			false,
		},
		"replace row deletion policy": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_TS1 TIMESTAMP NOT NULL,
			) PRIMARY KEY(T1_I1), ROW DELETION POLICY (OLDER_THAN(T1_TS1, INTERVAL 1 DAY));`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_TS1 TIMESTAMP NOT NULL,
			) PRIMARY KEY(T1_I1), ROW DELETION POLICY (OLDER_THAN(T1_TS1, INTERVAL 2 DAY));`,
			`
			ALTER TABLE T1 REPLACE ROW DELETION POLICY (OLDER_THAN(T1_TS1, INTERVAL 2 DAY));`,
			false,
		},
		"drop row deletion policy before column": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_T1 TIMESTAMP,
			  T1_T2 TIMESTAMP,
			) PRIMARY KEY(T1_I1), ROW DELETION POLICY (OLDER_THAN(T1_T1, INTERVAL 1 DAY));`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_T2 TIMESTAMP,
			) PRIMARY KEY(T1_I1), ROW DELETION POLICY (OLDER_THAN(T1_T2, INTERVAL 1 DAY));`,
			`
			ALTER TABLE T1 DROP ROW DELETION POLICY;
			ALTER TABLE T1 DROP COLUMN T1_T1;
			ALTER TABLE T1 ADD ROW DELETION POLICY (OLDER_THAN(T1_T2, INTERVAL 1 DAY));`,
			false,
		},
		"add synonym": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY (T1_I1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  SYNONYM(T2)
			) PRIMARY KEY (T1_I1)`,
			`
			ALTER TABLE T1 ADD SYNONYM T2;`,
			false,
		},
		"drop synonym": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  SYNONYM(T2)
			) PRIMARY KEY (T1_I1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY (T1_I1)`,
			`
			ALTER TABLE T1 DROP SYNONYM T2;`,
			false,
		},
		"recreate synonym": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  SYNONYM(T2)
			) PRIMARY KEY (T1_I1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  SYNONYM(T3)
			) PRIMARY KEY (T1_I1)`,
			`
			ALTER TABLE T1 DROP SYNONYM T2;
			ALTER TABLE T1 ADD SYNONYM T3;`,
			false,
		},
		"alter on delete action of interleaved table": {
			`
			CREATE TABLE P1 (
			  P1_I1 INT64 NOT NULL,
			) PRIMARY KEY(P1_I1);
			CREATE TABLE C1 (
			  P1_I1 INT64 NOT NULL,
			  C1_I1 INT64 NOT NULL,
			  C1_T1 TIMESTAMP,
			) PRIMARY KEY(P1_I1, C1_I1), INTERLEAVE IN PARENT P1 ON DELETE CASCADE;`,
			`
			CREATE TABLE P1 (
			  P1_I1 INT64 NOT NULL,
			) PRIMARY KEY(P1_I1);
			CREATE TABLE C1 (
			  P1_I1 INT64 NOT NULL,
			  C1_I1 INT64 NOT NULL,
			  C1_T1 TIMESTAMP,
			) PRIMARY KEY(P1_I1, C1_I1), INTERLEAVE IN PARENT P1, ROW DELETION POLICY (OLDER_THAN(C1_T1, INTERVAL 1 DAY));`,
			`
			ALTER TABLE C1 SET ON DELETE NO ACTION;
			ALTER TABLE C1 ADD ROW DELETION POLICY (OLDER_THAN(C1_T1, INTERVAL 1 DAY));`,
			false,
		},
		"alter interleave in parent to interleave in": {
			`
			CREATE TABLE P1 (
			  P1_I1 INT64 NOT NULL,
			) PRIMARY KEY(P1_I1);
			CREATE TABLE C1 (
			  P1_I1 INT64 NOT NULL,
			  C1_I1 INT64 NOT NULL,
			) PRIMARY KEY(P1_I1, C1_I1), INTERLEAVE IN PARENT P1 ON DELETE CASCADE;`,
			`
			CREATE TABLE P1 (
			  P1_I1 INT64 NOT NULL,
			) PRIMARY KEY(P1_I1);
			CREATE TABLE C1 (
			  P1_I1 INT64 NOT NULL,
			  C1_I1 INT64 NOT NULL,
			) PRIMARY KEY(P1_I1, C1_I1), INTERLEAVE IN P1;`,
			`
			ALTER TABLE C1 SET INTERLEAVE IN P1;`,
			false,
		},
		"alter interleave in to interleave in parent": {
			`
			CREATE TABLE P1 (
			  P1_I1 INT64 NOT NULL,
			) PRIMARY KEY(P1_I1);
			CREATE TABLE C1 (
			  P1_I1 INT64 NOT NULL,
			  C1_I1 INT64 NOT NULL,
			) PRIMARY KEY(P1_I1, C1_I1), INTERLEAVE IN P1;`,
			`
			CREATE TABLE P1 (
			  P1_I1 INT64 NOT NULL,
			) PRIMARY KEY(P1_I1);
			CREATE TABLE C1 (
			  P1_I1 INT64 NOT NULL,
			  C1_I1 INT64 NOT NULL,
			) PRIMARY KEY(P1_I1, C1_I1), INTERLEAVE IN PARENT P1;`,
			`
			ALTER TABLE C1 SET INTERLEAVE IN PARENT P1;`,
			false,
		},
		"match interleaved table without on delete with on delete no action": {
			`
			CREATE TABLE P1 (
			  P1_I1 INT64 NOT NULL,
			) PRIMARY KEY(P1_I1);
			CREATE TABLE C1 (
			  P1_I1 INT64 NOT NULL,
			  C1_I1 INT64 NOT NULL,
			) PRIMARY KEY(P1_I1, C1_I1), INTERLEAVE IN PARENT P1;`,
			`
			CREATE TABLE P1 (
			  P1_I1 INT64 NOT NULL,
			) PRIMARY KEY(P1_I1);
			CREATE TABLE C1 (
			  P1_I1 INT64 NOT NULL,
			  C1_I1 INT64 NOT NULL,
			) PRIMARY KEY(P1_I1, C1_I1), INTERLEAVE IN PARENT P1 ON DELETE NO ACTION;`,
			``,
			false,
		},
		"recreate table on interleave parent change": {
			`
			CREATE TABLE P1 (
			  P1_I1 INT64 NOT NULL,
			) PRIMARY KEY(P1_I1);
			CREATE TABLE C1 (
			  P1_I1 INT64 NOT NULL,
			  C1_I1 INT64 NOT NULL,
			) PRIMARY KEY(P1_I1, C1_I1), INTERLEAVE IN PARENT P1;`,
			`
			CREATE TABLE P1 (
			  P1_I1 INT64 NOT NULL,
			) PRIMARY KEY(P1_I1);
			CREATE TABLE C1 (
			  P1_I1 INT64 NOT NULL,
			  C1_I1 INT64 NOT NULL,
			) PRIMARY KEY(P1_I1, C1_I1);`,
			`
			DROP TABLE C1;
			CREATE TABLE C1 (
			  P1_I1 INT64 NOT NULL,
			  C1_I1 INT64 NOT NULL,
			) PRIMARY KEY(P1_I1, C1_I1);`,
			false,
		},
		"recreate dependencies by recreate table": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			  T1_AF1 ARRAY<FLOAT64> NOT NULL,
			) PRIMARY KEY (T1_I1);
			CREATE INDEX IDX1 ON T1(T1_I1);
			CREATE SEARCH INDEX IDX2 ON T1(T1_S1);
			CREATE CHANGE STREAM S1 FOR ALL;
			CREATE CHANGE STREAM S2 FOR T1;
			CREATE VECTOR INDEX IDX3 ON T1(T1_AF1) OPTIONS (distance_type = 'COSINE');
			GRANT SELECT ON TABLE T1 TO ROLE R1;
			`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			  T1_AF1 ARRAY<FLOAT64> NOT NULL,
			) PRIMARY KEY (T1_S1);
			CREATE INDEX IDX1 ON T1(T1_I1);
			CREATE SEARCH INDEX IDX2 ON T1(T1_S1);
			CREATE CHANGE STREAM S1 FOR ALL;
			CREATE CHANGE STREAM S2 FOR T1;
			CREATE VECTOR INDEX IDX3 ON T1(T1_AF1) OPTIONS (distance_type = 'COSINE');
			GRANT SELECT ON TABLE T1 TO ROLE R1;`,
			`
			DROP VECTOR INDEX IDX3;
			DROP SEARCH INDEX IDX2;
			DROP INDEX IDX1;
			REVOKE SELECT ON TABLE T1 FROM ROLE R1;
			ALTER CHANGE STREAM S2 DROP FOR ALL;
			DROP TABLE T1;
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			  T1_AF1 ARRAY<FLOAT64> NOT NULL,
			) PRIMARY KEY (T1_S1);
			ALTER CHANGE STREAM S2 SET FOR T1;
			GRANT SELECT ON TABLE T1 TO ROLE R1;
			CREATE INDEX IDX1 ON T1(T1_I1);
			CREATE SEARCH INDEX IDX2 ON T1(T1_S1);
			CREATE VECTOR INDEX IDX3 ON T1(T1_AF1) OPTIONS (distance_type = 'COSINE');`,
			false,
		},
		"add column": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			) PRIMARY KEY(T1_I1)`,
			`
			ALTER TABLE T1 ADD COLUMN T1_S1 STRING(MAX);`,
			false,
		},
		"add not null column": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX) NOT NULL,
			) PRIMARY KEY(T1_I1)`,
			``,
			true,
		},
		"add not null column with default": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX) NOT NULL DEFAULT ('a'),
			) PRIMARY KEY(T1_I1)`,
			`
			ALTER TABLE T1 ADD COLUMN T1_S1 STRING(MAX) NOT NULL DEFAULT ('a');`,
			false,
		},
		"drop column": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			) PRIMARY KEY(T1_I1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1)`,
			`
			ALTER TABLE T1 DROP COLUMN T1_S1;`,
			false,
		},
		"alter column": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			) PRIMARY KEY(T1_I1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(100),
			) PRIMARY KEY(T1_I1)`,
			`
			ALTER TABLE T1 ALTER COLUMN T1_S1 STRING(100);`,
			false,
		},
		"remove column options": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_T1 TIMESTAMP OPTIONS (allow_commit_timestamp = true),
			) PRIMARY KEY(T1_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_T1 TIMESTAMP,
			) PRIMARY KEY(T1_I1);`,
			`
			ALTER TABLE T1 ALTER COLUMN T1_T1 SET OPTIONS (allow_commit_timestamp = null);`,
			false,
		},
		"recreate column": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			) PRIMARY KEY(T1_I1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 INT64,
			) PRIMARY KEY(T1_I1)`,
			`
			ALTER TABLE T1 DROP COLUMN T1_S1;
			ALTER TABLE T1 ADD COLUMN T1_S1 INT64;`,
			false,
		},
		"recreate uuid columns": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_U1 UUID,
			  T1_U2 UUID,
			) PRIMARY KEY(T1_I1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_U1 BYTES(MAX),
			  T1_U2 examples.Message,
			) PRIMARY KEY(T1_I1)`,
			`
			ALTER TABLE T1 DROP COLUMN T1_U2;
			ALTER TABLE T1 DROP COLUMN T1_U1;
			ALTER TABLE T1 ADD COLUMN T1_U1 BYTES(MAX);
			ALTER TABLE T1 ADD COLUMN T1_U2 examples.Message;`,
			false,
		},
		"recreate generated column": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64,
			  T1_G1 INT64 AS (T1_I2 + 1) STORED,
			) PRIMARY KEY(T1_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64,
			  T1_G1 INT64 AS (T1_I2 + 2) STORED,
			) PRIMARY KEY(T1_I1);`,
			`
			ALTER TABLE T1 DROP COLUMN T1_G1;
			ALTER TABLE T1 ADD COLUMN T1_G1 INT64 AS (T1_I2 + 2) STORED;`,
			false,
		},
		"alter non-stored generated column": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64,
			  T1_G1 INT64 AS (T1_I2 + 1),
			) PRIMARY KEY(T1_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64,
			  T1_G1 INT64 AS (T1_I2 + 2),
			) PRIMARY KEY(T1_I1);`,
			`
			ALTER TABLE T1 ALTER COLUMN T1_G1 INT64 AS (T1_I2 + 2);`,
			false,
		},
		"recreate indexed non-stored generated column": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64,
			  T1_G1 INT64 AS (T1_I2 + 1),
			) PRIMARY KEY(T1_I1);
			CREATE INDEX IDX1 ON T1(T1_G1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64,
			  T1_G1 INT64 AS (T1_I2 + 2),
			) PRIMARY KEY(T1_I1);
			CREATE INDEX IDX1 ON T1(T1_G1);`,
			`
			DROP INDEX IDX1;
			ALTER TABLE T1 DROP COLUMN T1_G1;
			ALTER TABLE T1 ADD COLUMN T1_G1 INT64 AS (T1_I2 + 2);
			CREATE INDEX IDX1 ON T1(T1_G1);`,
			false,
		},
		"recreate column to not null column": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			) PRIMARY KEY(T1_I1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1)`,
			``,
			true,
		},
		"recreate column to generated column": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64,
			  T1_G1 INT64,
			) PRIMARY KEY(T1_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64,
			  T1_G1 INT64 NOT NULL AS (T1_I2 + 1) STORED,
			) PRIMARY KEY(T1_I1);`,
			`
			ALTER TABLE T1 DROP COLUMN T1_G1;
			ALTER TABLE T1 ADD COLUMN T1_G1 INT64 NOT NULL AS (T1_I2 + 1) STORED;`,
			false,
		},
		"add column referenced by new generated column": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			  T1_B1 BOOL AS (T1_S1 IS NOT NULL) STORED,
			) PRIMARY KEY(T1_I1);`,
			`
			ALTER TABLE T1 ADD COLUMN T1_B1 BOOL AS (T1_S1 IS NOT NULL) STORED;
			ALTER TABLE T1 ADD COLUMN T1_S1 STRING(MAX);`,
			false,
		},
		"drop generated column and column it references": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			  T1_B1 BOOL AS (T1_S1 IS NOT NULL) STORED,
			) PRIMARY KEY(T1_I1);
			CREATE INDEX IDX1 ON T1(T1_B1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1);`,
			`
			ALTER TABLE T1 DROP COLUMN T1_S1;
			DROP INDEX IDX1;
			ALTER TABLE T1 DROP COLUMN T1_B1;`,
			false,
		},
		"change type of column referenced by generated column": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			  T1_B1 BOOL AS (T1_S1 IS NOT NULL) STORED,
			) PRIMARY KEY(T1_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 BYTES(MAX),
			  T1_B1 BOOL AS (T1_S1 IS NOT NULL) STORED,
			) PRIMARY KEY(T1_I1);`,
			`
			ALTER TABLE T1 ALTER COLUMN T1_S1 BYTES(MAX);`,
			false,
		},
		"recreate column referenced by generated column": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			  T1_B1 BOOL AS (T1_S1 IS NOT NULL) STORED,
			) PRIMARY KEY(T1_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 INT64,
			  T1_B1 BOOL AS (T1_S1 IS NOT NULL) STORED,
			) PRIMARY KEY(T1_I1);`,
			`
			ALTER TABLE T1 DROP COLUMN T1_S1;
			ALTER TABLE T1 ADD COLUMN T1_S1 INT64;`,
			false,
		},
		"drop column that non-stored generated column stops referencing": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			  T1_S2 STRING(MAX),
			  T1_B1 BOOL AS (T1_S1 IS NOT NULL),
			) PRIMARY KEY(T1_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S2 STRING(MAX),
			  T1_B1 BOOL AS (T1_S2 IS NOT NULL),
			) PRIMARY KEY(T1_I1);`,
			`
			ALTER TABLE T1 DROP COLUMN T1_S1;
			ALTER TABLE T1 ALTER COLUMN T1_B1 BOOL AS (T1_S2 IS NOT NULL);`,
			false,
		},
		"recreate column that non-stored generated column stops referencing": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			  T1_S2 STRING(MAX),
			  T1_B1 BOOL AS (T1_S1 IS NOT NULL),
			) PRIMARY KEY(T1_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 INT64,
			  T1_S2 STRING(MAX),
			  T1_B1 BOOL AS (T1_S2 IS NOT NULL),
			) PRIMARY KEY(T1_I1);`,
			`
			ALTER TABLE T1 DROP COLUMN T1_S1;
			ALTER TABLE T1 ALTER COLUMN T1_B1 BOOL AS (T1_S2 IS NOT NULL);
			ALTER TABLE T1 ADD COLUMN T1_S1 INT64;`,
			false,
		},
		"drop column not referenced by generated column": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			  T1_S2 STRING(MAX),
			  T1_B1 BOOL AS (T1_S1 IS NOT NULL) STORED,
			) PRIMARY KEY(T1_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			  T1_B1 BOOL AS (T1_S1 IS NOT NULL) STORED,
			) PRIMARY KEY(T1_I1);`,
			`
			ALTER TABLE T1 DROP COLUMN T1_S2;`,
			false,
		},
		"change length of column referenced by generated column": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(10),
			  T1_B1 BOOL AS (T1_S1 IS NOT NULL) STORED,
			) PRIMARY KEY(T1_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			  T1_B1 BOOL AS (T1_S1 IS NOT NULL) STORED,
			) PRIMARY KEY(T1_I1);`,
			`
			ALTER TABLE T1 ALTER COLUMN T1_S1 STRING(MAX);`,
			false,
		},
		"error on converting column to identity column": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64,
			) PRIMARY KEY(T1_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64 GENERATED BY DEFAULT AS IDENTITY (BIT_REVERSED_POSITIVE),
			) PRIMARY KEY(T1_I1);`,
			``,
			true,
		},
		"error on converting identity column to column": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64 GENERATED BY DEFAULT AS IDENTITY (BIT_REVERSED_POSITIVE),
			) PRIMARY KEY(T1_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64,
			) PRIMARY KEY(T1_I1);`,
			``,
			true,
		},
		"set skip range of identity column": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL GENERATED BY DEFAULT AS IDENTITY (BIT_REVERSED_POSITIVE),
			) PRIMARY KEY(T1_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL GENERATED BY DEFAULT AS IDENTITY (BIT_REVERSED_POSITIVE SKIP RANGE 1, 1000),
			) PRIMARY KEY(T1_I1);`,
			`
			ALTER TABLE T1 ALTER COLUMN T1_I1 ALTER IDENTITY SET SKIP RANGE 1, 1000;`,
			false,
		},
		"change skip range of identity column": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL GENERATED BY DEFAULT AS IDENTITY (BIT_REVERSED_POSITIVE SKIP RANGE 1, 1000),
			) PRIMARY KEY(T1_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL GENERATED BY DEFAULT AS IDENTITY (BIT_REVERSED_POSITIVE SKIP RANGE 1, 2000),
			) PRIMARY KEY(T1_I1);`,
			`
			ALTER TABLE T1 ALTER COLUMN T1_I1 ALTER IDENTITY SET SKIP RANGE 1, 2000;`,
			false,
		},
		"unset skip range of identity column": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL GENERATED BY DEFAULT AS IDENTITY (BIT_REVERSED_POSITIVE SKIP RANGE 1, 1000),
			) PRIMARY KEY(T1_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL GENERATED BY DEFAULT AS IDENTITY (BIT_REVERSED_POSITIVE),
			) PRIMARY KEY(T1_I1);`,
			`
			ALTER TABLE T1 ALTER COLUMN T1_I1 ALTER IDENTITY SET NO SKIP RANGE;`,
			false,
		},
		"error on changing start counter of identity column": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL GENERATED BY DEFAULT AS IDENTITY (BIT_REVERSED_POSITIVE START COUNTER WITH 1000),
			) PRIMARY KEY(T1_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL GENERATED BY DEFAULT AS IDENTITY (BIT_REVERSED_POSITIVE START COUNTER WITH 2000),
			) PRIMARY KEY(T1_I1);`,
			``,
			true,
		},
		"ignore sequence kind of identity column": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL GENERATED BY DEFAULT AS IDENTITY (BIT_REVERSED_POSITIVE),
			) PRIMARY KEY(T1_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL AUTO_INCREMENT,
			) PRIMARY KEY(T1_I1);`,
			``,
			false,
		},
		"recreate table by recreating primary key column": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			) PRIMARY KEY(T1_I1);
			CREATE INDEX IDX1 ON T1(T1_S1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 STRING(MAX) NOT NULL,
			  T1_S1 STRING(MAX),
			) PRIMARY KEY(T1_I1);
			CREATE INDEX IDX1 ON T1(T1_S1);`,
			`
			DROP INDEX IDX1;
			DROP TABLE T1;
			CREATE TABLE T1 (
			  T1_I1 STRING(MAX) NOT NULL,
			  T1_S1 STRING(MAX),
			) PRIMARY KEY(T1_I1);
			CREATE INDEX IDX1 ON T1(T1_S1);`,
			false,
		},
		"add index": {
			``,
			`
			CREATE INDEX IDX1 ON T1(T1_S1)`,
			`
			CREATE INDEX IDX1 ON T1(T1_S1);`,
			false,
		},
		"add index on recreated column": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64,
			) PRIMARY KEY(T1_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 STRING(MAX),
			) PRIMARY KEY(T1_I1);
			CREATE INDEX IDX1 ON T1(T1_I2);`,
			`
			ALTER TABLE T1 DROP COLUMN T1_I2;
			ALTER TABLE T1 ADD COLUMN T1_I2 STRING(MAX);
			CREATE INDEX IDX1 ON T1(T1_I2);`,
			false,
		},
		"drop index": {
			`
			CREATE INDEX IDX1 ON T1(T1_S1)`,
			``,
			`
			DROP INDEX IDX1;`,
			false,
		},
		"recreate index": {
			`
			CREATE INDEX IDX1 ON T1(T1_I1)`,
			`
			CREATE INDEX IDX1 ON T1(T1_I1, T1_S1)`,
			`
			DROP INDEX IDX1;
			CREATE INDEX IDX1 ON T1(T1_I1, T1_S1);`,
			false,
		},
		"add index storing": {
			`
			CREATE INDEX IDX1 ON T1(T1_S1);`,
			`
			CREATE INDEX IDX1 ON T1(T1_S1) STORING (T1_I1);`,
			`
			ALTER INDEX IDX1 ADD STORED COLUMN T1_I1;`,
			false,
		},
		"drop index storing": {
			`
			CREATE INDEX IDX1 ON T1(T1_S1) STORING (T1_I1);`,
			`
			CREATE INDEX IDX1 ON T1(T1_S1);`,
			`
			ALTER INDEX IDX1 DROP STORED COLUMN T1_I1;`,
			false,
		},
		"drop index storing before column": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			  T1_S2 STRING(MAX),
			) PRIMARY KEY(T1_I1);
			CREATE INDEX IDX1 ON T1(T1_S1) STORING (T1_S2);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			) PRIMARY KEY(T1_I1);
			CREATE INDEX IDX1 ON T1(T1_S1);`,
			`
			ALTER INDEX IDX1 DROP STORED COLUMN T1_S2;
			ALTER TABLE T1 DROP COLUMN T1_S2;`,
			false,
		},
		"add expression index": {
			``,
			`
			CREATE INDEX IDX1 ON T1(T1_I1, (T1_S1 != ''))`,
			`
			CREATE INDEX IDX1 ON T1(T1_I1, (T1_S1 != ""));`,
			false,
		},
		"drop expression index": {
			`
			CREATE INDEX IDX1 ON T1(T1_I1, (T1_S1 != ''))`,
			``,
			`
			DROP INDEX IDX1;`,
			false,
		},
		"recreate expression index": {
			`
			CREATE INDEX IDX1 ON T1(T1_I1, (T1_S1 != ''))`,
			`
			CREATE INDEX IDX1 ON T1(T1_I1, (T1_S1 = ''))`,
			`
			DROP INDEX IDX1;
			CREATE INDEX IDX1 ON T1(T1_I1, (T1_S1 = ""));`,
			false,
		},
		"recreate expression index on recreated column": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64,
			) PRIMARY KEY(T1_I1);
			CREATE INDEX IDX1 ON T1((T1_I2 IS NULL));`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 STRING(MAX),
			) PRIMARY KEY(T1_I1);
			CREATE INDEX IDX1 ON T1((T1_I2 IS NULL));`,
			`
			DROP INDEX IDX1;
			ALTER TABLE T1 DROP COLUMN T1_I2;
			ALTER TABLE T1 ADD COLUMN T1_I2 STRING(MAX);
			CREATE INDEX IDX1 ON T1((T1_I2 IS NULL));`,
			false,
		},
		"drop expression index before column": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			) PRIMARY KEY(T1_I1);
			CREATE INDEX IDX1 ON T1((T1_S1 != ''));`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1);`,
			`
			DROP INDEX IDX1;
			ALTER TABLE T1 DROP COLUMN T1_S1;`,
			false,
		},
		"add search index": {
			``,
			`
			CREATE SEARCH INDEX IDX1 ON T1(T1_S1)`,
			`
			CREATE SEARCH INDEX IDX1 ON T1(T1_S1);`,
			false,
		},
		"add search index on recreated table": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64 NOT NULL,
			  T1_T1 TOKENLIST,
			) PRIMARY KEY(T1_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64 NOT NULL,
			  T1_T1 TOKENLIST,
			) PRIMARY KEY(T1_I1, T1_I2);
			CREATE SEARCH INDEX IDX1 ON T1(T1_T1);`,
			`
			DROP TABLE T1;
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64 NOT NULL,
			  T1_T1 TOKENLIST,
			) PRIMARY KEY(T1_I1, T1_I2);
			CREATE SEARCH INDEX IDX1 ON T1(T1_T1);`,
			false,
		},
		"drop search index": {
			`
			CREATE SEARCH INDEX IDX1 ON T1(T1_S1)`,
			``,
			`
			DROP SEARCH INDEX IDX1;`,
			false,
		},
		"recreate search index": {
			`
			CREATE SEARCH INDEX IDX1 ON T1(T1_I1)`,
			`
			CREATE SEARCH INDEX IDX1 ON T1(T1_I1, T1_S1)`,
			`
			DROP SEARCH INDEX IDX1;
			CREATE SEARCH INDEX IDX1 ON T1(T1_I1, T1_S1);`,
			false,
		},
		"add search index storing": {
			`
			CREATE SEARCH INDEX IDX1 ON T1(T1_S1);`,
			`
			CREATE SEARCH INDEX IDX1 ON T1(T1_S1) STORING (T1_I1);`,
			`
			ALTER SEARCH INDEX IDX1 ADD STORED COLUMN T1_I1;`,
			false,
		},
		"drop search index storing": {
			`
			CREATE SEARCH INDEX IDX1 ON T1(T1_S1) STORING (T1_I1);`,
			`
			CREATE SEARCH INDEX IDX1 ON T1(T1_S1);`,
			`
			ALTER SEARCH INDEX IDX1 DROP STORED COLUMN T1_I1;`,
			false,
		},
		"drop search index storing before column": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_T1 TOKENLIST,
			  T1_S1 STRING(MAX),
			) PRIMARY KEY(T1_I1);
			CREATE SEARCH INDEX SIDX1 ON T1(T1_T1) STORING (T1_S1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_T1 TOKENLIST,
			) PRIMARY KEY(T1_I1);
			CREATE SEARCH INDEX SIDX1 ON T1(T1_T1);`,
			`
			ALTER SEARCH INDEX SIDX1 DROP STORED COLUMN T1_S1;
			ALTER TABLE T1 DROP COLUMN T1_S1;`,
			false,
		},
		"add search index in named schema": {
			``,
			`
			CREATE SEARCH INDEX S1.IDX1 ON S1.T1(T1_S1)`,
			`
			CREATE SEARCH INDEX S1.IDX1 ON S1.T1(T1_S1);`,
			false,
		},
		"move search index to named schema": {
			`
			CREATE SEARCH INDEX IDX1 ON T1(T1_S1)`,
			`
			CREATE SEARCH INDEX S1.IDX1 ON S1.T1(T1_S1)`,
			`
			DROP SEARCH INDEX IDX1;
			CREATE SEARCH INDEX S1.IDX1 ON S1.T1(T1_S1);`,
			false,
		},
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
		"create view": {
			``,
			`
			CREATE VIEW V1 SQL SECURITY DEFINER AS SELECT * FROM T1;`,
			`
			CREATE VIEW V1 SQL SECURITY DEFINER AS SELECT * FROM T1;`,
			false,
		},
		"create view referencing recreated table": {
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
			CREATE VIEW V1 SQL SECURITY INVOKER AS SELECT T1_I1 FROM T1;`,
			`
			DROP TABLE T1;
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1, T1_I2);
			CREATE VIEW V1 SQL SECURITY INVOKER AS SELECT T1_I1 FROM T1;`,
			false,
		},
		"drop view": {
			`
			CREATE VIEW V1 SQL SECURITY DEFINER AS SELECT * FROM T1;`,
			``,
			`
			DROP VIEW V1;`,
			false,
		},
		"recreate view": {
			`
			CREATE VIEW V1 SQL SECURITY DEFINER AS SELECT * FROM T1;`,
			`
			CREATE VIEW V1 SQL SECURITY DEFINER AS SELECT * FROM T1 WHERE T1_I1 > 0;`,
			`
			CREATE OR REPLACE VIEW V1 SQL SECURITY DEFINER AS SELECT * FROM T1 WHERE T1_I1 > 0;`,
			false,
		},
		"drop and create view": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1);
			CREATE VIEW V1 SQL SECURITY DEFINER AS SELECT * FROM T1;`,
			`
			CREATE TABLE T1 (
			  T1_S1 STRING(MAX) NOT NULL,
			) PRIMARY KEY(T1_S1);
			CREATE VIEW V1 SQL SECURITY DEFINER AS SELECT * FROM T1;`,
			`
			DROP VIEW V1;
			DROP TABLE T1;
			CREATE TABLE T1 (
			  T1_S1 STRING(MAX) NOT NULL,
			) PRIMARY KEY(T1_S1);
			CREATE VIEW V1 SQL SECURITY DEFINER AS SELECT * FROM T1;`,
			false,
		},
		"recreate view referencing dropped table": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1);
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			) PRIMARY KEY(T2_I1);
			CREATE VIEW V1 SQL SECURITY INVOKER AS SELECT T1.T1_I1 FROM T1;`,
			`
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			) PRIMARY KEY(T2_I1);
			CREATE VIEW V1 SQL SECURITY INVOKER AS SELECT T2.T2_I1 FROM T2;`,
			`
			DROP VIEW V1;
			DROP TABLE T1;
			CREATE VIEW V1 SQL SECURITY INVOKER AS SELECT T2.T2_I1 FROM T2;`,
			false,
		},
		"add change stream": {
			``,
			`
			CREATE CHANGE STREAM S1 FOR ALL;`,
			`
			CREATE CHANGE STREAM S1 FOR ALL;`,
			false,
		},
		"add change stream for recreated table": {
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
			CREATE CHANGE STREAM S1 FOR T1;`,
			`
			DROP TABLE T1;
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1, T1_I2);
			CREATE CHANGE STREAM S1 FOR T1;`,
			false,
		},
		"drop change stream": {
			`
			CREATE CHANGE STREAM S1 FOR ALL;`,
			``,
			`
			DROP CHANGE STREAM S1;`,
			false,
		},
		"alter change stream": {
			`
			CREATE CHANGE STREAM S1 FOR ALL OPTIONS ( retention_period = '36h' );`,
			`
			CREATE CHANGE STREAM S1 FOR T1(T1_I1) OPTIONS ( retention_period = '72h' );`,
			`
			ALTER CHANGE STREAM S1 SET FOR T1(T1_I1);
			ALTER CHANGE STREAM S1 SET OPTIONS ( retention_period = '72h' );`,
			false,
		},
		"remove table from change stream before dropping table": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1);
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			) PRIMARY KEY(T2_I1);
			CREATE CHANGE STREAM CS1 FOR T1, T2;`,
			`
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			) PRIMARY KEY(T2_I1);
			CREATE TABLE T3 (
			  T3_I1 INT64 NOT NULL,
			) PRIMARY KEY(T3_I1);
			CREATE CHANGE STREAM CS1 FOR T2, T3;`,
			`
			ALTER CHANGE STREAM CS1 SET FOR T2;
			DROP TABLE T1;
			CREATE TABLE T3 (
			  T3_I1 INT64 NOT NULL,
			) PRIMARY KEY(T3_I1);
			ALTER CHANGE STREAM CS1 SET FOR T2, T3;`,
			false,
		},
		"drop change stream for": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1);
			CREATE CHANGE STREAM CS1 FOR T1;`,
			``,
			`
			DROP CHANGE STREAM CS1;
			DROP TABLE T1;`,
			false,
		},
		"remove all tables from change stream": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1);
			CREATE CHANGE STREAM CS1 FOR T1;`,
			`
			CREATE CHANGE STREAM CS1;`,
			`
			ALTER CHANGE STREAM CS1 DROP FOR ALL;
			DROP TABLE T1;`,
			false,
		},
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
		"add queue": {
			``,
			`
			CREATE QUEUE Q1 (
			  Q1_I1 INT64 NOT NULL,
			  Payload BYTES(MAX) NOT NULL,
			) PRIMARY KEY(Q1_I1)`,
			`
			CREATE QUEUE Q1 (
			  Q1_I1 INT64 NOT NULL,
			  Payload BYTES(MAX) NOT NULL,
			) PRIMARY KEY(Q1_I1);`,
			false,
		},
		"drop queue": {
			`
			CREATE QUEUE Q1 (
			  Q1_I1 INT64 NOT NULL,
			  Payload BYTES(MAX) NOT NULL,
			) PRIMARY KEY(Q1_I1)`,
			``,
			`
			DROP QUEUE Q1;`,
			false,
		},
		"recreate queue": {
			`
			CREATE QUEUE Q1 (
			  Q1_I1 INT64 NOT NULL,
			  Payload BYTES(MAX) NOT NULL,
			) PRIMARY KEY(Q1_I1)`,
			`
			CREATE QUEUE Q1 (
			  Q1_I1 INT64 NOT NULL,
			  Payload JSON NOT NULL,
			) PRIMARY KEY(Q1_I1)`,
			`
			DROP QUEUE Q1;
			CREATE QUEUE Q1 (
			  Q1_I1 INT64 NOT NULL,
			  Payload JSON NOT NULL,
			) PRIMARY KEY(Q1_I1);`,
			false,
		},
		"recreate queue by changing primary key": {
			`
			CREATE QUEUE Q1 (
			  Q1_I1 INT64 NOT NULL,
			  Payload BYTES(MAX) NOT NULL,
			) PRIMARY KEY(Q1_I1)`,
			`
			CREATE QUEUE Q1 (
			  Q1_I1 INT64 NOT NULL,
			  Q1_S1 STRING(36) NOT NULL,
			  Payload BYTES(MAX) NOT NULL,
			) PRIMARY KEY(Q1_I1, Q1_S1 DESC)`,
			`
			DROP QUEUE Q1;
			CREATE QUEUE Q1 (
			  Q1_I1 INT64 NOT NULL,
			  Q1_S1 STRING(36) NOT NULL,
			  Payload BYTES(MAX) NOT NULL,
			) PRIMARY KEY(Q1_I1, Q1_S1 DESC);`,
			false,
		},
		"ignore column order and default clauses of queue": {
			`
			CREATE TABLE P1 (
			  P1_I1 INT64 NOT NULL,
			) PRIMARY KEY(P1_I1);
			CREATE QUEUE Q1 (
			  P1_I1 INT64 NOT NULL,
			  Payload BYTES(MAX) NOT NULL,
			) PRIMARY KEY(P1_I1 ASC), INTERLEAVE IN PARENT P1 ON DELETE NO ACTION, OPTIONS (receive_mode = 'PULL');`,
			`
			CREATE TABLE P1 (
			  P1_I1 INT64 NOT NULL,
			) PRIMARY KEY(P1_I1);
			CREATE QUEUE Q1 (
			  Payload BYTES(MAX) NOT NULL,
			  P1_I1 INT64 NOT NULL,
			) PRIMARY KEY(P1_I1), INTERLEAVE IN PARENT P1, OPTIONS (receive_mode = 'PULL');`,
			``,
			false,
		},
		"alter queue options": {
			`
			CREATE QUEUE Q1 (
			  Q1_I1 INT64 NOT NULL,
			  Payload BYTES(MAX) NOT NULL,
			) PRIMARY KEY(Q1_I1), OPTIONS (receive_mode = 'PULL', locality_group = 'ssd')`,
			`
			CREATE QUEUE Q1 (
			  Q1_I1 INT64 NOT NULL,
			  Payload BYTES(MAX) NOT NULL,
			) PRIMARY KEY(Q1_I1), OPTIONS (receive_mode = 'PULL', disable_send = true)`,
			`
			ALTER QUEUE Q1 SET OPTIONS (disable_send = true, locality_group = null);`,
			false,
		},
		"add queue row deletion policy": {
			`
			CREATE QUEUE Q1 (
			  Q1_I1 INT64 NOT NULL,
			  Q1_T1 TIMESTAMP NOT NULL,
			  Payload BYTES(MAX) NOT NULL,
			) PRIMARY KEY(Q1_I1, Q1_T1)`,
			`
			CREATE QUEUE Q1 (
			  Q1_I1 INT64 NOT NULL,
			  Q1_T1 TIMESTAMP NOT NULL,
			  Payload BYTES(MAX) NOT NULL,
			) PRIMARY KEY(Q1_I1, Q1_T1), ROW DELETION POLICY (OLDER_THAN(Q1_T1, INTERVAL 7 DAY))`,
			`
			ALTER QUEUE Q1 ADD ROW DELETION POLICY (OLDER_THAN(Q1_T1, INTERVAL 7 DAY));`,
			false,
		},
		"replace queue row deletion policy": {
			`
			CREATE QUEUE Q1 (
			  Q1_I1 INT64 NOT NULL,
			  Q1_T1 TIMESTAMP NOT NULL,
			  Payload BYTES(MAX) NOT NULL,
			) PRIMARY KEY(Q1_I1, Q1_T1), ROW DELETION POLICY (OLDER_THAN(Q1_T1, INTERVAL 7 DAY))`,
			`
			CREATE QUEUE Q1 (
			  Q1_I1 INT64 NOT NULL,
			  Q1_T1 TIMESTAMP NOT NULL,
			  Payload BYTES(MAX) NOT NULL,
			) PRIMARY KEY(Q1_I1, Q1_T1), ROW DELETION POLICY (OLDER_THAN(Q1_T1, INTERVAL 1 DAY))`,
			`
			ALTER QUEUE Q1 REPLACE ROW DELETION POLICY (OLDER_THAN(Q1_T1, INTERVAL 1 DAY));`,
			false,
		},
		"drop queue row deletion policy": {
			`
			CREATE QUEUE Q1 (
			  Q1_I1 INT64 NOT NULL,
			  Q1_T1 TIMESTAMP NOT NULL,
			  Payload BYTES(MAX) NOT NULL,
			) PRIMARY KEY(Q1_I1, Q1_T1), ROW DELETION POLICY (OLDER_THAN(Q1_T1, INTERVAL 7 DAY))`,
			`
			CREATE QUEUE Q1 (
			  Q1_I1 INT64 NOT NULL,
			  Q1_T1 TIMESTAMP NOT NULL,
			  Payload BYTES(MAX) NOT NULL,
			) PRIMARY KEY(Q1_I1, Q1_T1)`,
			`
			ALTER QUEUE Q1 DROP ROW DELETION POLICY;`,
			false,
		},
		"drop queue with row deletion policy": {
			`
			CREATE QUEUE Q1 (
			  Q1_I1 INT64 NOT NULL,
			  Q1_T1 TIMESTAMP NOT NULL,
			  Payload BYTES(MAX) NOT NULL,
			) PRIMARY KEY(Q1_I1, Q1_T1), ROW DELETION POLICY (OLDER_THAN(Q1_T1, INTERVAL 7 DAY))`,
			``,
			`
			DROP QUEUE Q1;`,
			false,
		},
		"recreate queue with row deletion policy": {
			`
			CREATE QUEUE Q1 (
			  Q1_I1 INT64 NOT NULL,
			  Q1_T1 TIMESTAMP NOT NULL,
			  Payload BYTES(MAX) NOT NULL,
			) PRIMARY KEY(Q1_I1, Q1_T1), ROW DELETION POLICY (OLDER_THAN(Q1_T1, INTERVAL 7 DAY))`,
			`
			CREATE QUEUE Q1 (
			  Q1_I1 INT64 NOT NULL,
			  Q1_T1 TIMESTAMP NOT NULL,
			  Payload JSON NOT NULL,
			) PRIMARY KEY(Q1_I1, Q1_T1), ROW DELETION POLICY (OLDER_THAN(Q1_T1, INTERVAL 1 DAY))`,
			`
			DROP QUEUE Q1;
			CREATE QUEUE Q1 (
			  Q1_I1 INT64 NOT NULL,
			  Q1_T1 TIMESTAMP NOT NULL,
			  Payload JSON NOT NULL,
			) PRIMARY KEY(Q1_I1, Q1_T1), ROW DELETION POLICY (OLDER_THAN(Q1_T1, INTERVAL 1 DAY));`,
			false,
		},
		"alter on delete action of interleaved queue": {
			`
			CREATE TABLE P1 (
			  P1_I1 INT64 NOT NULL,
			) PRIMARY KEY(P1_I1);
			CREATE QUEUE Q1 (
			  P1_I1 INT64 NOT NULL,
			  Payload BYTES(MAX) NOT NULL,
			) PRIMARY KEY(P1_I1), INTERLEAVE IN PARENT P1;`,
			`
			CREATE TABLE P1 (
			  P1_I1 INT64 NOT NULL,
			) PRIMARY KEY(P1_I1);
			CREATE QUEUE Q1 (
			  P1_I1 INT64 NOT NULL,
			  Payload BYTES(MAX) NOT NULL,
			) PRIMARY KEY(P1_I1), INTERLEAVE IN PARENT P1 ON DELETE CASCADE;`,
			`
			ALTER QUEUE Q1 SET ON DELETE CASCADE;`,
			false,
		},
		"alter queue interleave in to interleave in parent": {
			`
			CREATE TABLE P1 (
			  P1_I1 INT64 NOT NULL,
			) PRIMARY KEY(P1_I1);
			CREATE QUEUE Q1 (
			  P1_I1 INT64 NOT NULL,
			  Payload BYTES(MAX) NOT NULL,
			) PRIMARY KEY(P1_I1), INTERLEAVE IN P1;`,
			`
			CREATE TABLE P1 (
			  P1_I1 INT64 NOT NULL,
			) PRIMARY KEY(P1_I1);
			CREATE QUEUE Q1 (
			  P1_I1 INT64 NOT NULL,
			  Payload BYTES(MAX) NOT NULL,
			) PRIMARY KEY(P1_I1), INTERLEAVE IN PARENT P1;`,
			`
			ALTER QUEUE Q1 SET INTERLEAVE IN PARENT P1;`,
			false,
		},
		"alter queue interleave in parent to interleave in": {
			`
			CREATE TABLE P1 (
			  P1_I1 INT64 NOT NULL,
			) PRIMARY KEY(P1_I1);
			CREATE QUEUE Q1 (
			  P1_I1 INT64 NOT NULL,
			  Payload BYTES(MAX) NOT NULL,
			) PRIMARY KEY(P1_I1), INTERLEAVE IN PARENT P1 ON DELETE CASCADE;`,
			`
			CREATE TABLE P1 (
			  P1_I1 INT64 NOT NULL,
			) PRIMARY KEY(P1_I1);
			CREATE QUEUE Q1 (
			  P1_I1 INT64 NOT NULL,
			  Payload BYTES(MAX) NOT NULL,
			) PRIMARY KEY(P1_I1), INTERLEAVE IN P1;`,
			`
			ALTER QUEUE Q1 SET INTERLEAVE IN P1;`,
			false,
		},
		"recreate queue on interleave parent change": {
			`
			CREATE TABLE P1 (
			  P1_I1 INT64 NOT NULL,
			) PRIMARY KEY(P1_I1);
			CREATE TABLE P2 (
			  P1_I1 INT64 NOT NULL,
			) PRIMARY KEY(P1_I1);
			CREATE QUEUE Q1 (
			  P1_I1 INT64 NOT NULL,
			  Payload BYTES(MAX) NOT NULL,
			) PRIMARY KEY(P1_I1), INTERLEAVE IN P1;`,
			`
			CREATE TABLE P1 (
			  P1_I1 INT64 NOT NULL,
			) PRIMARY KEY(P1_I1);
			CREATE TABLE P2 (
			  P1_I1 INT64 NOT NULL,
			) PRIMARY KEY(P1_I1);
			CREATE QUEUE Q1 (
			  P1_I1 INT64 NOT NULL,
			  Payload BYTES(MAX) NOT NULL,
			) PRIMARY KEY(P1_I1), INTERLEAVE IN P2;`,
			`
			DROP QUEUE Q1;
			CREATE QUEUE Q1 (
			  P1_I1 INT64 NOT NULL,
			  Payload BYTES(MAX) NOT NULL,
			) PRIMARY KEY(P1_I1), INTERLEAVE IN P2;`,
			false,
		},
		"recreate queue on adding interleave": {
			`
			CREATE TABLE P1 (
			  P1_I1 INT64 NOT NULL,
			) PRIMARY KEY(P1_I1);
			CREATE QUEUE Q1 (
			  P1_I1 INT64 NOT NULL,
			  Payload BYTES(MAX) NOT NULL,
			) PRIMARY KEY(P1_I1);`,
			`
			CREATE TABLE P1 (
			  P1_I1 INT64 NOT NULL,
			) PRIMARY KEY(P1_I1);
			CREATE QUEUE Q1 (
			  P1_I1 INT64 NOT NULL,
			  Payload BYTES(MAX) NOT NULL,
			) PRIMARY KEY(P1_I1), INTERLEAVE IN P1;`,
			`
			DROP QUEUE Q1;
			CREATE QUEUE Q1 (
			  P1_I1 INT64 NOT NULL,
			  Payload BYTES(MAX) NOT NULL,
			) PRIMARY KEY(P1_I1), INTERLEAVE IN P1;`,
			false,
		},
		"add interleaved queue": {
			``,
			`
			CREATE QUEUE Q1 (
			  P1_I1 INT64 NOT NULL,
			  Payload BYTES(MAX) NOT NULL,
			) PRIMARY KEY(P1_I1), INTERLEAVE IN PARENT P1 ON DELETE CASCADE;
			CREATE TABLE P1 (
			  P1_I1 INT64 NOT NULL,
			) PRIMARY KEY(P1_I1);`,
			`
			CREATE TABLE P1 (
			  P1_I1 INT64 NOT NULL,
			) PRIMARY KEY(P1_I1);
			CREATE QUEUE Q1 (
			  P1_I1 INT64 NOT NULL,
			  Payload BYTES(MAX) NOT NULL,
			) PRIMARY KEY(P1_I1), INTERLEAVE IN PARENT P1 ON DELETE CASCADE;`,
			false,
		},
		"add queue interleaved in recreated table": {
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
			CREATE QUEUE Q1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64 NOT NULL,
			  Payload BYTES(MAX) NOT NULL,
			) PRIMARY KEY(T1_I1, T1_I2), INTERLEAVE IN PARENT T1;`,
			`
			DROP TABLE T1;
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1, T1_I2);
			CREATE QUEUE Q1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64 NOT NULL,
			  Payload BYTES(MAX) NOT NULL,
			) PRIMARY KEY(T1_I1, T1_I2), INTERLEAVE IN PARENT T1;`,
			false,
		},
		"drop interleaved queue": {
			`
			CREATE TABLE P1 (
			  P1_I1 INT64 NOT NULL,
			) PRIMARY KEY(P1_I1);
			CREATE QUEUE Q1 (
			  P1_I1 INT64 NOT NULL,
			  Payload BYTES(MAX) NOT NULL,
			) PRIMARY KEY(P1_I1), INTERLEAVE IN PARENT P1 ON DELETE CASCADE;`,
			``,
			`
			DROP QUEUE Q1;
			DROP TABLE P1;`,
			false,
		},
		"recreate interleaved queue by recreating parent table": {
			`
			CREATE TABLE G1 (
			  G1_I1 INT64 NOT NULL,
			) PRIMARY KEY(G1_I1);
			CREATE TABLE G2 (
			  G1_I1 INT64 NOT NULL,
			) PRIMARY KEY(G1_I1);
			CREATE TABLE P1 (
			  G1_I1 INT64 NOT NULL,
			  P1_I1 INT64 NOT NULL,
			) PRIMARY KEY(G1_I1, P1_I1), INTERLEAVE IN G1;
			CREATE QUEUE Q1 (
			  G1_I1 INT64 NOT NULL,
			  P1_I1 INT64 NOT NULL,
			  Payload BYTES(MAX) NOT NULL,
			) PRIMARY KEY(G1_I1, P1_I1), INTERLEAVE IN PARENT P1;`,
			`
			CREATE TABLE G1 (
			  G1_I1 INT64 NOT NULL,
			) PRIMARY KEY(G1_I1);
			CREATE TABLE G2 (
			  G1_I1 INT64 NOT NULL,
			) PRIMARY KEY(G1_I1);
			CREATE TABLE P1 (
			  G1_I1 INT64 NOT NULL,
			  P1_I1 INT64 NOT NULL,
			) PRIMARY KEY(G1_I1, P1_I1), INTERLEAVE IN G2;
			CREATE QUEUE Q1 (
			  G1_I1 INT64 NOT NULL,
			  P1_I1 INT64 NOT NULL,
			  Payload BYTES(MAX) NOT NULL,
			) PRIMARY KEY(G1_I1, P1_I1), INTERLEAVE IN PARENT P1;`,
			`
			DROP QUEUE Q1;
			DROP TABLE P1;
			CREATE TABLE P1 (
			  G1_I1 INT64 NOT NULL,
			  P1_I1 INT64 NOT NULL,
			) PRIMARY KEY(G1_I1, P1_I1), INTERLEAVE IN G2;
			CREATE QUEUE Q1 (
			  G1_I1 INT64 NOT NULL,
			  P1_I1 INT64 NOT NULL,
			  Payload BYTES(MAX) NOT NULL,
			) PRIMARY KEY(G1_I1, P1_I1), INTERLEAVE IN PARENT P1;`,
			false,
		},
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
		"identifiers are case-insensitive": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX) OPTIONS (allow_commit_timestamp = false),
			  CONSTRAINT C1 CHECK (T1_I1 > 0),
			  SYNONYM(S1),
			) PRIMARY KEY(T1_I1);
			CREATE INDEX IDX1 ON T1(T1_S1) STORING (T1_I1);
			CREATE INDEX IDX2 ON T1((T1_S1 != ''));
			CREATE VIEW V1 SQL SECURITY INVOKER AS SELECT T1.T1_I1 FROM T1;
			CREATE QUEUE Q1 (
			  T1_I1 INT64 NOT NULL,
			  Payload BYTES(MAX) NOT NULL,
			) PRIMARY KEY(T1_I1), INTERLEAVE IN PARENT T1, OPTIONS (receive_mode = 'PULL');
			GRANT SELECT(T1_S1) ON TABLE T1 TO ROLE R1;`,
			`
			create table t1 (
			  t1_i1 int64 not null,
			  t1_s1 string(max) options (ALLOW_COMMIT_TIMESTAMP = false),
			  constraint c1 check (t1_i1 > 0),
			  synonym(s1),
			) primary key(t1_i1);
			create index idx1 on t1(t1_s1) storing (t1_i1);
			create index idx2 on t1((t1_s1 != ''));
			create view v1 sql security invoker as select t1.t1_i1 from t1;
			create queue q1 (
			  t1_i1 int64 not null,
			  payload bytes(max) not null,
			) primary key(t1_i1), interleave in parent t1, options (RECEIVE_MODE = 'PULL');
			grant select(t1_s1) on table t1 to role r1;`,
			``,
			false,
		},
		"proto type names are case-sensitive": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_P1 examples.Message,
			) PRIMARY KEY(T1_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_P1 examples.message,
			) PRIMARY KEY(T1_I1);`,
			`
			ALTER TABLE T1 ALTER COLUMN T1_P1 examples.message;`,
			false,
		},
		"ignore IF NOT EXISTS and OR REPLACE": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1);
			CREATE INDEX IDX1 ON T1(T1_I1);
			CREATE SEQUENCE S1 OPTIONS (sequence_kind = 'bit_reversed_positive');
			CREATE VIEW V1 SQL SECURITY INVOKER AS SELECT T1.T1_I1 FROM T1;`,
			`
			CREATE TABLE IF NOT EXISTS T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1);
			CREATE INDEX IF NOT EXISTS IDX1 ON T1(T1_I1);
			CREATE SEQUENCE IF NOT EXISTS S1 OPTIONS (sequence_kind = 'bit_reversed_positive');
			CREATE OR REPLACE VIEW V1 SQL SECURITY INVOKER AS SELECT T1.T1_I1 FROM T1;`,
			``,
			false,
		},
		"issue #35": { // https://github.com/morikuni/spannerdiff/issues/35
			``,
			`
			CREATE OR REPLACE VIEW V2 SQL SECURITY INVOKER AS SELECT * FROM T1;
			CREATE OR REPLACE VIEW V1 SQL SECURITY INVOKER AS SELECT * FROM V2;`,
			`
			CREATE OR REPLACE VIEW V2 SQL SECURITY INVOKER AS SELECT * FROM T1;
			CREATE OR REPLACE VIEW V1 SQL SECURITY INVOKER AS SELECT * FROM V2;`,
			false,
		},
		"issue #37": { // https://github.com/morikuni/spannerdiff/issues/37
			`
			CREATE INDEX IDX1 ON T1 (T1_I1)`,
			`
			CREATE INDEX IDX1 ON T1 (T1_I1 ASC)`,
			``,
			false,
		},
		"drop interleaved tables": {
			`
			CREATE TABLE Parent (
			  ID INT64 NOT NULL,
			) PRIMARY KEY(ID);
			CREATE TABLE Child (
			  ID INT64 NOT NULL,
			  ChildID INT64 NOT NULL,
			) PRIMARY KEY(ID, ChildID), INTERLEAVE IN PARENT Parent ON DELETE CASCADE;`,
			``,
			`
			DROP TABLE Child;
			DROP TABLE Parent;`,
			false,
		},
		"add interleaved tables": {
			``,
			`
			CREATE TABLE Parent (
			  ID INT64 NOT NULL,
			) PRIMARY KEY(ID);
			CREATE TABLE Child (
			  ID INT64 NOT NULL,
			  ChildID INT64 NOT NULL,
			) PRIMARY KEY(ID, ChildID), INTERLEAVE IN PARENT Parent ON DELETE CASCADE;`,
			`
			CREATE TABLE Parent (
			  ID INT64 NOT NULL,
			) PRIMARY KEY(ID);
			CREATE TABLE Child (
			  ID INT64 NOT NULL,
			  ChildID INT64 NOT NULL,
			) PRIMARY KEY(ID, ChildID), INTERLEAVE IN PARENT Parent ON DELETE CASCADE;`,
			false,
		},
		"add table interleaved in recreated table": {
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
			CREATE TABLE C1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64 NOT NULL,
			  C1_I1 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1, T1_I2, C1_I1), INTERLEAVE IN PARENT T1;`,
			`
			DROP TABLE T1;
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1, T1_I2);
			CREATE TABLE C1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64 NOT NULL,
			  C1_I1 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1, T1_I2, C1_I1), INTERLEAVE IN PARENT T1;`,
			false,
		},
		"recreate interleaved parent table": {
			`
			CREATE TABLE Parent (
			  ID INT64 NOT NULL,
			) PRIMARY KEY(ID);
			CREATE TABLE Child (
			  ID INT64 NOT NULL,
			  ChildID INT64 NOT NULL,
			) PRIMARY KEY(ID, ChildID), INTERLEAVE IN PARENT Parent ON DELETE CASCADE;`,
			`
			CREATE TABLE Parent (
			  ID INT64 NOT NULL,
			  ID2 INT64 NOT NULL,
			) PRIMARY KEY(ID, ID2);
			CREATE TABLE Child (
			  ID INT64 NOT NULL,
			  ChildID INT64 NOT NULL,
			) PRIMARY KEY(ID, ChildID), INTERLEAVE IN PARENT Parent ON DELETE CASCADE;`,
			`
			DROP TABLE Child;
			DROP TABLE Parent;
			CREATE TABLE Parent (
			  ID INT64 NOT NULL,
			  ID2 INT64 NOT NULL,
			) PRIMARY KEY(ID, ID2);
			CREATE TABLE Child (
			  ID INT64 NOT NULL,
			  ChildID INT64 NOT NULL,
			) PRIMARY KEY(ID, ChildID), INTERLEAVE IN PARENT Parent ON DELETE CASCADE;`,
			false,
		},
		"add tables with inline foreign keys referencing each other": {
			``,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64,
			  CONSTRAINT FK1 FOREIGN KEY (T1_I2) REFERENCES T2 (T2_I1),
			) PRIMARY KEY(T1_I1);
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			  T2_I2 INT64,
			  CONSTRAINT FK2 FOREIGN KEY (T2_I2) REFERENCES T1 (T1_I1),
			) PRIMARY KEY(T2_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64
			) PRIMARY KEY (T1_I1);
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			  T2_I2 INT64
			) PRIMARY KEY (T2_I1);
			ALTER TABLE T1 ADD CONSTRAINT FK1 FOREIGN KEY (T1_I2) REFERENCES T2 (T2_I1);
			ALTER TABLE T2 ADD CONSTRAINT FK2 FOREIGN KEY (T2_I2) REFERENCES T1 (T1_I1);`,
			false,
		},
		"drop tables with inline foreign keys referencing each other": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64,
			  CONSTRAINT FK1 FOREIGN KEY (T1_I2) REFERENCES T2 (T2_I1),
			) PRIMARY KEY(T1_I1);
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			  T2_I2 INT64,
			  CONSTRAINT FK2 FOREIGN KEY (T2_I2) REFERENCES T1 (T1_I1),
			) PRIMARY KEY(T2_I1);`,
			``,
			`
			ALTER TABLE T2 DROP CONSTRAINT FK2;
			ALTER TABLE T1 DROP CONSTRAINT FK1;
			DROP TABLE T2;
			DROP TABLE T1;`,
			false,
		},
		"match unnamed foreign key added by alter table with inline foreign key": {
			`
			CREATE TABLE T1 (
			  T1_S1 STRING(MAX) NOT NULL,
			) PRIMARY KEY(T1_S1);
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			  T2_S1 STRING(MAX) NOT NULL,
			) PRIMARY KEY(T2_I1);
			ALTER TABLE T2 ADD FOREIGN KEY(T2_S1) REFERENCES T1(T1_S1) ON DELETE NO ACTION;`,
			`
			CREATE TABLE T1 (
			  T1_S1 STRING(MAX) NOT NULL,
			) PRIMARY KEY(T1_S1);
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			  T2_S1 STRING(MAX) NOT NULL,
			  FOREIGN KEY (T2_S1) REFERENCES T1 (T1_S1) ON DELETE NO ACTION,
			) PRIMARY KEY(T2_I1);`,
			``,
			false,
		},
		"match unnamed check constraint added by alter table with inline check constraint": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1);
			ALTER TABLE T1 ADD CHECK (T1_I1 > 0);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  CHECK (T1_I1 > 0),
			) PRIMARY KEY(T1_I1);`,
			``,
			false,
		},
		"match constraint added by alter table before create table": {
			`
			ALTER TABLE T1 ADD CHECK (T1_I1 > 0);
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  CHECK (T1_I1 > 0),
			) PRIMARY KEY(T1_I1);`,
			``,
			false,
		},
		"drop named foreign key added by alter table": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			) PRIMARY KEY(T1_I1);
			ALTER TABLE T1 ADD CONSTRAINT FK1 FOREIGN KEY (T1_S1) REFERENCES T2 (T2_S1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			) PRIMARY KEY(T1_I1);`,
			`
			ALTER TABLE T1 DROP CONSTRAINT FK1;`,
			false,
		},
		"add foreign key by alter table in target": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			) PRIMARY KEY(T1_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			) PRIMARY KEY(T1_I1);
			ALTER TABLE T1 ADD CONSTRAINT FK1 FOREIGN KEY (T1_S1) REFERENCES T2 (T2_S1);`,
			`
			ALTER TABLE T1 ADD CONSTRAINT FK1 FOREIGN KEY (T1_S1) REFERENCES T2(T2_S1);`,
			false,
		},
		"error on adding constraint to unknown table by alter table": {
			``,
			`
			ALTER TABLE T1 ADD CHECK (T1_I1 > 0);`,
			``,
			true,
		},
		"match constraint added by alter table on table in named schema": {
			`
			CREATE SCHEMA S1;
			CREATE TABLE S1.T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1);
			ALTER TABLE S1.T1 ADD CHECK (T1_I1 > 0);`,
			`
			CREATE SCHEMA S1;
			CREATE TABLE S1.T1 (
			  T1_I1 INT64 NOT NULL,
			  CHECK (T1_I1 > 0),
			) PRIMARY KEY(T1_I1);`,
			``,
			false,
		},
		"add constraints by alter table with new table": {
			`
			CREATE TABLE T1 (
			  T1_S1 STRING(MAX) NOT NULL,
			) PRIMARY KEY(T1_S1);`,
			`
			CREATE TABLE T1 (
			  T1_S1 STRING(MAX) NOT NULL,
			) PRIMARY KEY(T1_S1);
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			  T2_S1 STRING(MAX) NOT NULL,
			) PRIMARY KEY(T2_I1);
			ALTER TABLE T2 ADD CONSTRAINT FK1 FOREIGN KEY (T2_S1) REFERENCES T1 (T1_S1);
			ALTER TABLE T2 ADD CHECK (T2_I1 > 0);`,
			`
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			  T2_S1 STRING(MAX) NOT NULL,
			  CHECK (T2_I1 > 0)
			) PRIMARY KEY (T2_I1);
			ALTER TABLE T2 ADD CONSTRAINT FK1 FOREIGN KEY (T2_S1) REFERENCES T1(T1_S1);`,
			false,
		},
		"add constraint by alter table again with recreated table": {
			`
			CREATE TABLE T1 (
			  T1_S1 STRING(MAX) NOT NULL,
			) PRIMARY KEY(T1_S1);
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			  T2_S1 STRING(MAX) NOT NULL,
			) PRIMARY KEY(T2_I1);
			ALTER TABLE T2 ADD CONSTRAINT FK1 FOREIGN KEY (T2_S1) REFERENCES T1 (T1_S1);`,
			`
			CREATE TABLE T1 (
			  T1_S1 STRING(MAX) NOT NULL,
			) PRIMARY KEY(T1_S1);
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			  T2_S1 STRING(MAX) NOT NULL,
			) PRIMARY KEY(T2_I1, T2_S1);
			ALTER TABLE T2 ADD CONSTRAINT FK1 FOREIGN KEY (T2_S1) REFERENCES T1 (T1_S1);`,
			`
			ALTER TABLE T2 DROP CONSTRAINT FK1;
			DROP TABLE T2;
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			  T2_S1 STRING(MAX) NOT NULL
			) PRIMARY KEY (T2_I1, T2_S1);
			ALTER TABLE T2 ADD CONSTRAINT FK1 FOREIGN KEY (T2_S1) REFERENCES T1(T1_S1);`,
			false,
		},
		"add constraint by alter table with new table referencing recreated table": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX) NOT NULL,
			) PRIMARY KEY(T1_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX) NOT NULL,
			) PRIMARY KEY(T1_I1, T1_S1);
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			  T2_S1 STRING(MAX) NOT NULL,
			) PRIMARY KEY(T2_I1);
			ALTER TABLE T2 ADD CONSTRAINT FK1 FOREIGN KEY (T2_S1) REFERENCES T1 (T1_S1);`,
			`
			DROP TABLE T1;
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX) NOT NULL
			) PRIMARY KEY (T1_I1, T1_S1);
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			  T2_S1 STRING(MAX) NOT NULL
			) PRIMARY KEY (T2_I1);
			ALTER TABLE T2 ADD CONSTRAINT FK1 FOREIGN KEY (T2_S1) REFERENCES T1(T1_S1);`,
			false,
		},
		"add foreign key by alter table again with recreated tables": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1);
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			) PRIMARY KEY(T2_I1);
			ALTER TABLE T2 ADD CONSTRAINT FK1 FOREIGN KEY (T2_I1) REFERENCES T1 (T1_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 STRING(MAX) NOT NULL,
			) PRIMARY KEY(T1_I1);
			CREATE TABLE T2 (
			  T2_I1 STRING(MAX) NOT NULL,
			) PRIMARY KEY(T2_I1);
			ALTER TABLE T2 ADD CONSTRAINT FK1 FOREIGN KEY (T2_I1) REFERENCES T1 (T1_I1);`,
			`
			ALTER TABLE T2 DROP CONSTRAINT FK1;
			DROP TABLE T2;
			DROP TABLE T1;
			CREATE TABLE T1 (
			  T1_I1 STRING(MAX) NOT NULL
			) PRIMARY KEY (T1_I1);
			CREATE TABLE T2 (
			  T2_I1 STRING(MAX) NOT NULL
			) PRIMARY KEY (T2_I1);
			ALTER TABLE T2 ADD CONSTRAINT FK1 FOREIGN KEY (T2_I1) REFERENCES T1(T1_I1);`,
			false,
		},
		"add circular foreign keys by alter table with new tables": {
			``,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64,
			) PRIMARY KEY(T1_I1);
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			  T2_I2 INT64,
			) PRIMARY KEY(T2_I1);
			ALTER TABLE T1 ADD CONSTRAINT FK1 FOREIGN KEY (T1_I2) REFERENCES T2 (T2_I1);
			ALTER TABLE T2 ADD CONSTRAINT FK2 FOREIGN KEY (T2_I2) REFERENCES T1 (T1_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64
			) PRIMARY KEY (T1_I1);
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			  T2_I2 INT64
			) PRIMARY KEY (T2_I1);
			ALTER TABLE T1 ADD CONSTRAINT FK1 FOREIGN KEY (T1_I2) REFERENCES T2(T2_I1);
			ALTER TABLE T2 ADD CONSTRAINT FK2 FOREIGN KEY (T2_I2) REFERENCES T1(T1_I1);`,
			false,
		},
		"add constraint by alter table again with recreated interleaved table": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1);
			CREATE TABLE T2 (
			  T1_I1 INT64 NOT NULL,
			  T2_I1 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1, T2_I1),
			  INTERLEAVE IN PARENT T1 ON DELETE CASCADE;
			ALTER TABLE T2 ADD CHECK (T2_I1 > 0);`,
			`
			CREATE TABLE T1 (
			  T1_I1 STRING(MAX) NOT NULL,
			) PRIMARY KEY(T1_I1);
			CREATE TABLE T2 (
			  T1_I1 STRING(MAX) NOT NULL,
			  T2_I1 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1, T2_I1),
			  INTERLEAVE IN PARENT T1 ON DELETE CASCADE;
			ALTER TABLE T2 ADD CHECK (T2_I1 > 0);`,
			`
			DROP TABLE T2;
			DROP TABLE T1;
			CREATE TABLE T1 (
			  T1_I1 STRING(MAX) NOT NULL
			) PRIMARY KEY (T1_I1);
			CREATE TABLE T2 (
			  T1_I1 STRING(MAX) NOT NULL,
			  T2_I1 INT64 NOT NULL,
			  CHECK (T2_I1 > 0)
			) PRIMARY KEY (T1_I1, T2_I1),
			  INTERLEAVE IN PARENT T1 ON DELETE CASCADE;`,
			false,
		},
		"drop named foreign key by alter table before dropping tables": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64,
			) PRIMARY KEY(T1_I1);
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			) PRIMARY KEY(T2_I1);
			ALTER TABLE T1 ADD CONSTRAINT FK1 FOREIGN KEY (T1_I2) REFERENCES T2 (T2_I1);`,
			``,
			`
			ALTER TABLE T1 DROP CONSTRAINT FK1;
			DROP TABLE T2;
			DROP TABLE T1;`,
			false,
		},
		"drop table with unnamed foreign key by alter table before referenced table": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64,
			) PRIMARY KEY(T1_I1);
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			) PRIMARY KEY(T2_I1);
			ALTER TABLE T1 ADD FOREIGN KEY (T1_I2) REFERENCES T2 (T2_I1);`,
			``,
			`
			DROP TABLE T1;
			DROP TABLE T2;`,
			false,
		},
		"recreate tables with unnamed foreign key by alter table": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64,
			) PRIMARY KEY(T1_I1);
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			) PRIMARY KEY(T2_I1);
			ALTER TABLE T1 ADD FOREIGN KEY (T1_I2) REFERENCES T2 (T2_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 STRING(MAX) NOT NULL,
			  T1_I2 INT64,
			) PRIMARY KEY(T1_I1);
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			  T2_I2 INT64,
			) PRIMARY KEY(T2_I1, T2_I2);
			ALTER TABLE T1 ADD FOREIGN KEY (T1_I2) REFERENCES T2 (T2_I1);`,
			`
			DROP TABLE T1;
			DROP TABLE T2;
			CREATE TABLE T1 (
			  T1_I1 STRING(MAX) NOT NULL,
			  T1_I2 INT64
			) PRIMARY KEY (T1_I1);
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			  T2_I2 INT64
			) PRIMARY KEY (T2_I1, T2_I2);
			ALTER TABLE T1 ADD FOREIGN KEY (T1_I2) REFERENCES T2(T2_I1);`,
			false,
		},
		"drop table with unnamed foreign key by alter table before referenced table in schema": {
			`
			CREATE SCHEMA S1;
			CREATE TABLE S1.T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64,
			) PRIMARY KEY(T1_I1);
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			) PRIMARY KEY(T2_I1);
			ALTER TABLE S1.T1 ADD FOREIGN KEY (T1_I2) REFERENCES T2 (T2_I1);`,
			`
			CREATE TABLE T2 (
			  T2_I1 STRING(MAX) NOT NULL,
			) PRIMARY KEY(T2_I1);`,
			`
			DROP TABLE S1.T1;
			DROP TABLE T2;
			DROP SCHEMA S1;
			CREATE TABLE T2 (
			  T2_I1 STRING(MAX) NOT NULL
			) PRIMARY KEY (T2_I1);`,
			false,
		},
		"drop circular named foreign keys by alter table before dropping tables": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64,
			) PRIMARY KEY(T1_I1);
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			  T2_I2 INT64,
			) PRIMARY KEY(T2_I1);
			ALTER TABLE T1 ADD CONSTRAINT FK1 FOREIGN KEY (T1_I2) REFERENCES T2 (T2_I1);
			ALTER TABLE T2 ADD CONSTRAINT FK2 FOREIGN KEY (T2_I2) REFERENCES T1 (T1_I1);`,
			``,
			`
			ALTER TABLE T2 DROP CONSTRAINT FK2;
			ALTER TABLE T1 DROP CONSTRAINT FK1;
			DROP TABLE T2;
			DROP TABLE T1;`,
			false,
		},
		"error on dropping tables with circular unnamed foreign keys by alter table": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64,
			) PRIMARY KEY(T1_I1);
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			  T2_I2 INT64,
			) PRIMARY KEY(T2_I1);
			ALTER TABLE T1 ADD FOREIGN KEY (T1_I2) REFERENCES T2 (T2_I1);
			ALTER TABLE T2 ADD FOREIGN KEY (T2_I2) REFERENCES T1 (T1_I1);`,
			``,
			``,
			true,
		},
		"recreate table with named foreign key by alter table into inline foreign key": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64,
			) PRIMARY KEY(T1_I1);
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			) PRIMARY KEY(T2_I1);
			ALTER TABLE T1 ADD CONSTRAINT FK1 FOREIGN KEY (T1_I2) REFERENCES T2 (T2_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 STRING(MAX) NOT NULL,
			  T1_I2 INT64,
			  CONSTRAINT FK1 FOREIGN KEY (T1_I2) REFERENCES T2 (T2_I1),
			) PRIMARY KEY(T1_I1);
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			) PRIMARY KEY(T2_I1);`,
			`
			ALTER TABLE T1 DROP CONSTRAINT FK1;
			DROP TABLE T1;
			CREATE TABLE T1 (
			  T1_I1 STRING(MAX) NOT NULL,
			  T1_I2 INT64
			) PRIMARY KEY (T1_I1);
			ALTER TABLE T1 ADD CONSTRAINT FK1 FOREIGN KEY (T1_I2) REFERENCES T2 (T2_I1);`,
			false,
		},
		"add foreign key by alter table referencing recreated table": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64,
			) PRIMARY KEY(T1_I1);
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			  T2_I2 INT64,
			) PRIMARY KEY(T2_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64,
			) PRIMARY KEY(T1_I1);
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			  T2_I2 INT64,
			) PRIMARY KEY(T2_I1, T2_I2);
			ALTER TABLE T1 ADD CONSTRAINT FK1 FOREIGN KEY (T1_I2) REFERENCES T2 (T2_I1);`,
			`
			DROP TABLE T2;
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			  T2_I2 INT64
			) PRIMARY KEY (T2_I1, T2_I2);
			ALTER TABLE T1 ADD CONSTRAINT FK1 FOREIGN KEY (T1_I2) REFERENCES T2(T2_I1);`,
			false,
		},
		"add check constraint by alter table on recreated column": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64,
			) PRIMARY KEY(T1_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 STRING(MAX),
			) PRIMARY KEY(T1_I1);
			ALTER TABLE T1 ADD CONSTRAINT C1 CHECK (T1_I2 != '');`,
			`
			ALTER TABLE T1 DROP COLUMN T1_I2;
			ALTER TABLE T1 ADD COLUMN T1_I2 STRING(MAX);
			ALTER TABLE T1 ADD CONSTRAINT C1 CHECK (T1_I2 != "");`,
			false,
		},
		"drop table with unnamed foreign key by alter table before referenced column": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64,
			) PRIMARY KEY(T1_I1);
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			  T2_I2 INT64,
			) PRIMARY KEY(T2_I1);
			ALTER TABLE T1 ADD FOREIGN KEY (T1_I2) REFERENCES T2 (T2_I2);`,
			`
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			) PRIMARY KEY(T2_I1);`,
			`
			DROP TABLE T1;
			ALTER TABLE T2 DROP COLUMN T2_I2;`,
			false,
		},
		"drop table with unnamed foreign key by alter table before recreating referenced column": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64,
			) PRIMARY KEY(T1_I1);
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			  T2_I2 INT64,
			) PRIMARY KEY(T2_I1);
			ALTER TABLE T1 ADD FOREIGN KEY (T1_I2) REFERENCES T2 (T2_I2);`,
			`
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			  T2_I2 STRING(MAX),
			) PRIMARY KEY(T2_I1);`,
			`
			DROP TABLE T1;
			ALTER TABLE T2 DROP COLUMN T2_I2;
			ALTER TABLE T2 ADD COLUMN T2_I2 STRING(MAX);`,
			false,
		},
		"drop table with named check constraint by alter table": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1);
			ALTER TABLE T1 ADD CONSTRAINT C1 CHECK (T1_I1 > 0);`,
			``,
			`
			DROP TABLE T1;`,
			false,
		},
		"drop table with named self-referencing foreign key by alter table": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64,
			) PRIMARY KEY(T1_I1);
			ALTER TABLE T1 ADD CONSTRAINT FK1 FOREIGN KEY (T1_I2) REFERENCES T1 (T1_I1);`,
			``,
			`
			DROP TABLE T1;`,
			false,
		},
		"recreate table with named foreign key by alter table into unnamed inline foreign key": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64,
			) PRIMARY KEY(T1_I1);
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			) PRIMARY KEY(T2_I1);
			ALTER TABLE T1 ADD CONSTRAINT FK_T1 FOREIGN KEY (T1_I2) REFERENCES T2 (T2_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 STRING(MAX) NOT NULL,
			  T1_I2 INT64,
			  FOREIGN KEY (T1_I2) REFERENCES T2 (T2_I1),
			) PRIMARY KEY(T1_I1);
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			) PRIMARY KEY(T2_I1);`,
			`
			ALTER TABLE T1 DROP CONSTRAINT FK_T1;
			DROP TABLE T1;
			CREATE TABLE T1 (
			  T1_I1 STRING(MAX) NOT NULL,
			  T1_I2 INT64
			) PRIMARY KEY (T1_I1);
			ALTER TABLE T1 ADD FOREIGN KEY (T1_I2) REFERENCES T2 (T2_I1);`,
			false,
		},
		"recreate table with named inline foreign key into unnamed foreign key by alter table": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64,
			  CONSTRAINT FK_T1 FOREIGN KEY (T1_I2) REFERENCES T2 (T2_I1),
			) PRIMARY KEY(T1_I1);
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			) PRIMARY KEY(T2_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 STRING(MAX) NOT NULL,
			  T1_I2 INT64,
			) PRIMARY KEY(T1_I1);
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			) PRIMARY KEY(T2_I1);
			ALTER TABLE T1 ADD FOREIGN KEY (T1_I2) REFERENCES T2 (T2_I1);`,
			`
			ALTER TABLE T1 DROP CONSTRAINT FK_T1;
			DROP TABLE T1;
			CREATE TABLE T1 (
			  T1_I1 STRING(MAX) NOT NULL,
			  T1_I2 INT64
			) PRIMARY KEY (T1_I1);
			ALTER TABLE T1 ADD FOREIGN KEY (T1_I2) REFERENCES T2(T2_I1);`,
			false,
		},
		"match named foreign key by alter table with unnamed foreign key by alter table": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64,
			) PRIMARY KEY(T1_I1);
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			) PRIMARY KEY(T2_I1);
			ALTER TABLE T1 ADD CONSTRAINT FK_T1 FOREIGN KEY (T1_I2) REFERENCES T2 (T2_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64,
			) PRIMARY KEY(T1_I1);
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			) PRIMARY KEY(T2_I1);
			ALTER TABLE T1 ADD FOREIGN KEY (T1_I2) REFERENCES T2 (T2_I1);`,
			``,
			false,
		},
		"match named foreign key added by alter table with unnamed inline foreign key": {
			`
			CREATE TABLE T1 (
			  T1_S1 STRING(MAX) NOT NULL,
			) PRIMARY KEY(T1_S1);
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			  T2_S1 STRING(MAX) NOT NULL,
			) PRIMARY KEY(T2_I1);
			ALTER TABLE T2 ADD CONSTRAINT FK_T2 FOREIGN KEY(T2_S1) REFERENCES T1(T1_S1) ON DELETE NO ACTION;`,
			`
			CREATE TABLE T1 (
			  T1_S1 STRING(MAX) NOT NULL,
			) PRIMARY KEY(T1_S1);
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			  T2_S1 STRING(MAX) NOT NULL,
			  FOREIGN KEY (T2_S1) REFERENCES T1 (T1_S1) ON DELETE NO ACTION,
			) PRIMARY KEY(T2_I1);`,
			``,
			false,
		},
		"drop named foreign key by alter table with the same definition as unnamed foreign key by alter table": {
			`
			CREATE TABLE T1 (
			  T1_S1 STRING(MAX) NOT NULL,
			) PRIMARY KEY(T1_S1);
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			  T2_S1 STRING(MAX) NOT NULL,
			) PRIMARY KEY(T2_I1);
			ALTER TABLE T2 ADD FOREIGN KEY(T2_S1) REFERENCES T1(T1_S1) ON DELETE NO ACTION;
			ALTER TABLE T2 ADD CONSTRAINT FK_T2 FOREIGN KEY(T2_S1) REFERENCES T1(T1_S1) ON DELETE NO ACTION;`,
			`
			CREATE TABLE T1 (
			  T1_S1 STRING(MAX) NOT NULL,
			) PRIMARY KEY(T1_S1);
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			  T2_S1 STRING(MAX) NOT NULL,
			  FOREIGN KEY (T2_S1) REFERENCES T1 (T1_S1) ON DELETE NO ACTION,
			) PRIMARY KEY(T2_I1);`,
			`
			ALTER TABLE T2 DROP CONSTRAINT FK_T2;`,
			false,
		},
		"error on naming unnamed foreign key added by alter table": {
			`
			CREATE TABLE T1 (
			  T1_S1 STRING(MAX) NOT NULL,
			) PRIMARY KEY(T1_S1);
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			  T2_S1 STRING(MAX) NOT NULL,
			) PRIMARY KEY(T2_I1);
			ALTER TABLE T2 ADD FOREIGN KEY(T2_S1) REFERENCES T1(T1_S1) ON DELETE NO ACTION;`,
			`
			CREATE TABLE T1 (
			  T1_S1 STRING(MAX) NOT NULL,
			) PRIMARY KEY(T1_S1);
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			  T2_S1 STRING(MAX) NOT NULL,
			  CONSTRAINT FK_T2 FOREIGN KEY (T2_S1) REFERENCES T1 (T1_S1) ON DELETE NO ACTION,
			) PRIMARY KEY(T2_I1);`,
			``,
			true,
		},
		"drop tables with named and unnamed foreign keys by alter table referencing each other": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64,
			) PRIMARY KEY(T1_I1);
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			  T2_I2 INT64,
			) PRIMARY KEY(T2_I1);
			ALTER TABLE T1 ADD CONSTRAINT FK1 FOREIGN KEY (T1_I2) REFERENCES T2 (T2_I1);
			ALTER TABLE T2 ADD FOREIGN KEY (T2_I2) REFERENCES T1 (T1_I1);`,
			``,
			`
			ALTER TABLE T1 DROP CONSTRAINT FK1;
			DROP TABLE T2;
			DROP TABLE T1;`,
			false,
		},
		"drop tables with unnamed foreign key by alter table and named inline foreign key referencing each other": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64,
			) PRIMARY KEY(T1_I1);
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			  T2_I2 INT64,
			  CONSTRAINT FK2 FOREIGN KEY (T2_I2) REFERENCES T1 (T1_I1),
			) PRIMARY KEY(T2_I1);
			ALTER TABLE T1 ADD FOREIGN KEY (T1_I2) REFERENCES T2 (T2_I1);`,
			``,
			`
			ALTER TABLE T2 DROP CONSTRAINT FK2;
			DROP TABLE T1;
			DROP TABLE T2;`,
			false,
		},
		"add unnamed foreign key by alter table again with recreated interleaved table and referenced table": {
			`
			CREATE TABLE P1 (
			  P1_I1 INT64 NOT NULL,
			) PRIMARY KEY(P1_I1);
			CREATE TABLE C1 (
			  P1_I1 INT64 NOT NULL,
			  C1_I1 INT64 NOT NULL,
			) PRIMARY KEY(P1_I1, C1_I1),
			  INTERLEAVE IN PARENT P1;
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			) PRIMARY KEY(T2_I1);
			ALTER TABLE C1 ADD FOREIGN KEY (C1_I1) REFERENCES T2 (T2_I1);`,
			`
			CREATE TABLE P1 (
			  P1_I1 STRING(MAX) NOT NULL,
			) PRIMARY KEY(P1_I1);
			CREATE TABLE C1 (
			  P1_I1 STRING(MAX) NOT NULL,
			  C1_I1 INT64 NOT NULL,
			) PRIMARY KEY(P1_I1, C1_I1),
			  INTERLEAVE IN PARENT P1;
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			  T2_I2 INT64 NOT NULL,
			) PRIMARY KEY(T2_I1, T2_I2);
			ALTER TABLE C1 ADD FOREIGN KEY (C1_I1) REFERENCES T2 (T2_I1);`,
			`
			DROP TABLE C1;
			DROP TABLE T2;
			DROP TABLE P1;
			CREATE TABLE P1 (
			  P1_I1 STRING(MAX) NOT NULL
			) PRIMARY KEY (P1_I1);
			CREATE TABLE C1 (
			  P1_I1 STRING(MAX) NOT NULL,
			  C1_I1 INT64 NOT NULL
			) PRIMARY KEY (P1_I1, C1_I1),
			  INTERLEAVE IN PARENT P1;
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			  T2_I2 INT64 NOT NULL
			) PRIMARY KEY (T2_I1, T2_I2);
			ALTER TABLE C1 ADD FOREIGN KEY (C1_I1) REFERENCES T2(T2_I1);`,
			false,
		},
		"recreate named foreign key by alter table with recreated referenced table": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64,
			) PRIMARY KEY(T1_I1);
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			  T2_I2 INT64,
			) PRIMARY KEY(T2_I1);
			ALTER TABLE T1 ADD CONSTRAINT FK1 FOREIGN KEY (T1_I2) REFERENCES T2 (T2_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64,
			) PRIMARY KEY(T1_I1);
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			  T2_I2 INT64,
			) PRIMARY KEY(T2_I1, T2_I2);
			ALTER TABLE T1 ADD CONSTRAINT FK1 FOREIGN KEY (T1_I2) REFERENCES T2 (T2_I1);`,
			`
			ALTER TABLE T1 DROP CONSTRAINT FK1;
			DROP TABLE T2;
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			  T2_I2 INT64
			) PRIMARY KEY (T2_I1, T2_I2);
			ALTER TABLE T1 ADD CONSTRAINT FK1 FOREIGN KEY (T1_I2) REFERENCES T2(T2_I1);`,
			false,
		},
		"error on recreating referenced table of unnamed foreign key by alter table": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64,
			) PRIMARY KEY(T1_I1);
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			  T2_I2 INT64,
			) PRIMARY KEY(T2_I1);
			ALTER TABLE T1 ADD FOREIGN KEY (T1_I2) REFERENCES T2 (T2_I1);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64,
			) PRIMARY KEY(T1_I1);
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			  T2_I2 INT64,
			) PRIMARY KEY(T2_I1, T2_I2);
			ALTER TABLE T1 ADD FOREIGN KEY (T1_I2) REFERENCES T2 (T2_I1);`,
			``,
			true,
		},
		"error on adding constraint to unknown table by alter table in base": {
			`
			ALTER TABLE T1 ADD CHECK (T1_I1 > 0);`,
			``,
			``,
			true,
		},
		"match constraint added by alter table with table name in different case and backquotes": {
			"CREATE TABLE T1 (T1_I1 INT64 NOT NULL) PRIMARY KEY(T1_I1);\n" +
				"ALTER TABLE t1 ADD CHECK (T1_I1 > 0);\n" +
				"ALTER TABLE `T1` ADD CHECK (T1_I1 < 10);",
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  CHECK (T1_I1 > 0),
			  CHECK (T1_I1 < 10),
			) PRIMARY KEY(T1_I1);`,
			``,
			false,
		},
		"drop table with foreign key and column it references": {
			`
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			  T2_S1 STRING(MAX),
			) PRIMARY KEY(T2_I1);
			CREATE TABLE T3 (
			  T3_S1 STRING(MAX) NOT NULL,
			  FOREIGN KEY (T3_S1) REFERENCES T2 (T2_S1),
			) PRIMARY KEY(T3_S1);
			CREATE TABLE T1 (
			  T1_S1 STRING(MAX) NOT NULL,
			  FOREIGN KEY (T1_S1) REFERENCES T3 (T3_S1),
			) PRIMARY KEY(T1_S1)`,
			`
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			) PRIMARY KEY(T2_I1)`,
			`
			DROP TABLE T1;
			DROP TABLE T3;
			ALTER TABLE T2 DROP COLUMN T2_S1;`,
			false,
		},
		"create table with foreign key and column it references": {
			`
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			) PRIMARY KEY(T2_I1)`,
			`
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			  T2_S1 STRING(MAX),
			) PRIMARY KEY(T2_I1);
			CREATE TABLE T3 (
			  T3_S1 STRING(MAX) NOT NULL,
			  FOREIGN KEY (T3_S1) REFERENCES T2 (T2_S1),
			) PRIMARY KEY(T3_S1);
			CREATE TABLE T1 (
			  T1_S1 STRING(MAX) NOT NULL,
			  FOREIGN KEY (T1_S1) REFERENCES T3 (T3_S1),
			) PRIMARY KEY(T1_S1)`,
			`
			CREATE TABLE T1 (
			  T1_S1 STRING(MAX) NOT NULL
			) PRIMARY KEY (T1_S1);
			CREATE TABLE T3 (
			  T3_S1 STRING(MAX) NOT NULL
			) PRIMARY KEY (T3_S1);
			ALTER TABLE T1 ADD FOREIGN KEY (T1_S1) REFERENCES T3 (T3_S1);
			ALTER TABLE T2 ADD COLUMN T2_S1 STRING(MAX);
			ALTER TABLE T3 ADD FOREIGN KEY (T3_S1) REFERENCES T2 (T2_S1);`,
			false,
		},
		"drop table with foreign key and recreate column it references": {
			`
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			  T2_S1 STRING(MAX),
			) PRIMARY KEY(T2_I1);
			CREATE TABLE T3 (
			  T3_S1 STRING(MAX) NOT NULL,
			  FOREIGN KEY (T3_S1) REFERENCES T2 (T2_S1),
			) PRIMARY KEY(T3_S1);
			CREATE TABLE T1 (
			  T1_S1 STRING(MAX) NOT NULL,
			  FOREIGN KEY (T1_S1) REFERENCES T3 (T3_S1),
			) PRIMARY KEY(T1_S1)`,
			`
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			  T2_S1 INT64,
			) PRIMARY KEY(T2_I1)`,
			`
			DROP TABLE T1;
			DROP TABLE T3;
			ALTER TABLE T2 DROP COLUMN T2_S1;
			ALTER TABLE T2 ADD COLUMN T2_S1 INT64;`,
			false,
		},
		"create table with foreign key and recreate column it references": {
			`
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			  T2_S1 INT64,
			) PRIMARY KEY(T2_I1)`,
			`
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			  T2_S1 STRING(MAX),
			) PRIMARY KEY(T2_I1);
			CREATE TABLE T3 (
			  T3_S1 STRING(MAX) NOT NULL,
			  FOREIGN KEY (T3_S1) REFERENCES T2 (T2_S1),
			) PRIMARY KEY(T3_S1);
			CREATE TABLE T1 (
			  T1_S1 STRING(MAX) NOT NULL,
			  FOREIGN KEY (T1_S1) REFERENCES T3 (T3_S1),
			) PRIMARY KEY(T1_S1)`,
			`
			ALTER TABLE T2 DROP COLUMN T2_S1;
			CREATE TABLE T1 (
			  T1_S1 STRING(MAX) NOT NULL
			) PRIMARY KEY (T1_S1);
			CREATE TABLE T3 (
			  T3_S1 STRING(MAX) NOT NULL
			) PRIMARY KEY (T3_S1);
			ALTER TABLE T1 ADD FOREIGN KEY (T1_S1) REFERENCES T3 (T3_S1);
			ALTER TABLE T2 ADD COLUMN T2_S1 STRING(MAX);
			ALTER TABLE T3 ADD FOREIGN KEY (T3_S1) REFERENCES T2 (T2_S1);`,
			false,
		},
	} {
		t.Run(name, func(t *testing.T) {
			var buf bytes.Buffer
			err := Diff(strings.NewReader(tt.base), strings.NewReader(tt.target), &buf, DiffOption{
				ErrorOnUnsupportedDDL: true,
			})
			if tt.wantError {
				if err == nil {
					t.Fatalf("want error, got nil")
				}
				return
			} else if err != nil {
				t.Fatalf("want no error, got %v", err)
			}

			if (err != nil) != tt.wantError {
				t.Fatalf("want error %v, got %v", tt.wantError, err)
			}
			equalDDLs(t, tt.wantDDLs, buf.String())
		})
	}
}

func TestDiffZeroDefaultForNotNullColumns(t *testing.T) {
	for name, tt := range map[string]struct {
		base      string
		target    string
		wantDDLs  string
		wantError bool
	}{
		"add not null columns": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_BL1 BOOL NOT NULL,
			  T1_I2 INT64 NOT NULL,
			  T1_F1 FLOAT32 NOT NULL,
			  T1_F2 FLOAT64 NOT NULL,
			  T1_N1 NUMERIC NOT NULL,
			  T1_S1 STRING(MAX) NOT NULL,
			  T1_S2 STRING(10) NOT NULL,
			  T1_BY1 BYTES(MAX) NOT NULL,
			  T1_D1 DATE NOT NULL,
			  T1_T1 TIMESTAMP NOT NULL,
			  T1_J1 JSON NOT NULL,
			  T1_AF1 ARRAY<FLOAT64> NOT NULL,
			  T1_U1 UUID NOT NULL,
			  T1_AU1 ARRAY<UUID> NOT NULL,
			) PRIMARY KEY(T1_I1)`,
			`
			ALTER TABLE T1 ADD COLUMN T1_AF1 ARRAY<FLOAT64> NOT NULL DEFAULT (ARRAY[]);
			ALTER TABLE T1 ALTER COLUMN T1_AF1 DROP DEFAULT;
			ALTER TABLE T1 ADD COLUMN T1_AU1 ARRAY<UUID> NOT NULL DEFAULT (ARRAY[]);
			ALTER TABLE T1 ALTER COLUMN T1_AU1 DROP DEFAULT;
			ALTER TABLE T1 ADD COLUMN T1_BL1 BOOL NOT NULL DEFAULT (FALSE);
			ALTER TABLE T1 ALTER COLUMN T1_BL1 DROP DEFAULT;
			ALTER TABLE T1 ADD COLUMN T1_BY1 BYTES(MAX) NOT NULL DEFAULT (b'');
			ALTER TABLE T1 ALTER COLUMN T1_BY1 DROP DEFAULT;
			ALTER TABLE T1 ADD COLUMN T1_D1 DATE NOT NULL DEFAULT (DATE '0001-01-01');
			ALTER TABLE T1 ALTER COLUMN T1_D1 DROP DEFAULT;
			ALTER TABLE T1 ADD COLUMN T1_F1 FLOAT32 NOT NULL DEFAULT (0);
			ALTER TABLE T1 ALTER COLUMN T1_F1 DROP DEFAULT;
			ALTER TABLE T1 ADD COLUMN T1_F2 FLOAT64 NOT NULL DEFAULT (0);
			ALTER TABLE T1 ALTER COLUMN T1_F2 DROP DEFAULT;
			ALTER TABLE T1 ADD COLUMN T1_I2 INT64 NOT NULL DEFAULT (0);
			ALTER TABLE T1 ALTER COLUMN T1_I2 DROP DEFAULT;
			ALTER TABLE T1 ADD COLUMN T1_J1 JSON NOT NULL DEFAULT (JSON '{}');
			ALTER TABLE T1 ALTER COLUMN T1_J1 DROP DEFAULT;
			ALTER TABLE T1 ADD COLUMN T1_N1 NUMERIC NOT NULL DEFAULT (NUMERIC '0');
			ALTER TABLE T1 ALTER COLUMN T1_N1 DROP DEFAULT;
			ALTER TABLE T1 ADD COLUMN T1_S1 STRING(MAX) NOT NULL DEFAULT ('');
			ALTER TABLE T1 ALTER COLUMN T1_S1 DROP DEFAULT;
			ALTER TABLE T1 ADD COLUMN T1_S2 STRING(10) NOT NULL DEFAULT ('');
			ALTER TABLE T1 ALTER COLUMN T1_S2 DROP DEFAULT;
			ALTER TABLE T1 ADD COLUMN T1_T1 TIMESTAMP NOT NULL DEFAULT (TIMESTAMP '0001-01-01T00:00:00Z');
			ALTER TABLE T1 ALTER COLUMN T1_T1 DROP DEFAULT;
			ALTER TABLE T1 ADD COLUMN T1_U1 UUID NOT NULL DEFAULT (CAST('00000000-0000-0000-0000-000000000000' AS UUID));
			ALTER TABLE T1 ALTER COLUMN T1_U1 DROP DEFAULT;`,
			false,
		},
		"add not null commit timestamp column": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_T1 TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
			) PRIMARY KEY(T1_I1)`,
			`
			ALTER TABLE T1 ADD COLUMN T1_T1 TIMESTAMP NOT NULL DEFAULT (TIMESTAMP '0001-01-01T00:00:00Z');
			ALTER TABLE T1 ALTER COLUMN T1_T1 DROP DEFAULT;
			ALTER TABLE T1 ALTER COLUMN T1_T1 SET OPTIONS (allow_commit_timestamp = true);`,
			false,
		},
		"add not null column with default": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX) NOT NULL DEFAULT ('a'),
			) PRIMARY KEY(T1_I1)`,
			`
			ALTER TABLE T1 ADD COLUMN T1_S1 STRING(MAX) NOT NULL DEFAULT ('a');`,
			false,
		},
		"add not null proto column": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_P1 examples.shipping.Order NOT NULL,
			) PRIMARY KEY(T1_I1)`,
			``,
			true,
		},
		"recreate column to not null column": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			) PRIMARY KEY(T1_I1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 INT64 NOT NULL,
			) PRIMARY KEY(T1_I1)`,
			`
			ALTER TABLE T1 DROP COLUMN T1_S1;
			ALTER TABLE T1 ADD COLUMN T1_S1 INT64 NOT NULL DEFAULT (0);
			ALTER TABLE T1 ALTER COLUMN T1_S1 DROP DEFAULT;`,
			false,
		},
	} {
		t.Run(name, func(t *testing.T) {
			var buf bytes.Buffer
			err := Diff(strings.NewReader(tt.base), strings.NewReader(tt.target), &buf, DiffOption{
				ErrorOnUnsupportedDDL:        true,
				ZeroDefaultForNotNullColumns: true,
			})
			if tt.wantError {
				if err == nil {
					t.Fatalf("want error, got nil")
				}
				return
			} else if err != nil {
				t.Fatalf("want no error, got %v", err)
			}
			equalDDLs(t, tt.wantDDLs, buf.String())
		})
	}
}

func equalDDLs(t *testing.T, a, b string) {
	t.Helper()
	ddlsA, err := memefish.ParseDDLs("a", a)
	if err != nil {
		t.Fatalf("failed to parse ddl a: %v", err)
	}
	ddlsB, err := memefish.ParseDDLs("b", b)
	if err != nil {
		t.Fatalf("failed to parse ddl b: %v", err)
	}
	linesA := make([]string, 0, len(ddlsA))
	for _, ddl := range ddlsA {
		linesA = append(linesA, ddl.SQL())
	}
	linesB := make([]string, 0, len(ddlsB))
	for _, ddl := range ddlsB {
		linesB = append(linesB, ddl.SQL())
	}
	if diff := cmp.Diff(linesA, linesB); diff != "" {
		t.Errorf("diff (+got -want):\n%s", diff)
	}
}

func TestDiffDeterministic(t *testing.T) {
	for name, tt := range map[string]struct {
		base   string
		target string
	}{
		"constraints, synonyms, row deletion policy and index": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64 NOT NULL,
			  T1_I3 INT64 NOT NULL,
			  T1_I4 INT64 NOT NULL,
			  T1_T1 TIMESTAMP NOT NULL,
			  CONSTRAINT C1 CHECK (T1_I1 > 0),
			  CONSTRAINT C2 CHECK (T1_I2 > 0),
			  SYNONYM(S1),
			) PRIMARY KEY(T1_I1);
			CREATE INDEX IDX1 ON T1(T1_I2) STORING (T1_I3, T1_I4);`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64 NOT NULL,
			  T1_I3 INT64 NOT NULL,
			  T1_I4 INT64 NOT NULL,
			  T1_T1 TIMESTAMP NOT NULL,
			  CONSTRAINT C3 CHECK (T1_I3 > 0),
			  CONSTRAINT C4 CHECK (T1_I4 > 0),
			  SYNONYM(S2),
			) PRIMARY KEY(T1_I1), ROW DELETION POLICY (OLDER_THAN(T1_T1, INTERVAL 1 DAY));
			CREATE INDEX IDX1 ON T1(T1_I2) STORING (T1_T1, T1_I1);`,
		},
		"foreign keys with on delete": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			  T1_S2 STRING(MAX),
			  CONSTRAINT FK1 FOREIGN KEY (T1_S1) REFERENCES T2 (T2_S1),
			  CONSTRAINT FK2 FOREIGN KEY (T1_S1) REFERENCES T2 (T2_S1) ON DELETE CASCADE,
			  FOREIGN KEY (T1_S2) REFERENCES T2 (T2_S1),
			  FOREIGN KEY (T1_S2) REFERENCES T2 (T2_S1) ON DELETE CASCADE,
			) PRIMARY KEY(T1_I1)`,
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_S1 STRING(MAX),
			  T1_S2 STRING(MAX),
			  FOREIGN KEY (T1_S1) REFERENCES T2 (T2_S1) ON DELETE NO ACTION,
			  FOREIGN KEY (T1_S1) REFERENCES T2 (T2_S1) ON DELETE CASCADE,
			  FOREIGN KEY (T1_S2) REFERENCES T2 (T2_S1) ON DELETE NO ACTION,
			  FOREIGN KEY (T1_S2) REFERENCES T2 (T2_S1) ON DELETE CASCADE,
			  CONSTRAINT FK3 FOREIGN KEY (T1_S2) REFERENCES T2 (T2_S1) ON DELETE NO ACTION NOT ENFORCED,
			) PRIMARY KEY(T1_I1)`,
		},
		"constraints by alter table": {
			`
			CREATE TABLE T1 (
			  T1_I1 INT64 NOT NULL,
			  T1_I2 INT64,
			  T1_I3 INT64,
			) PRIMARY KEY(T1_I1);
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			  T2_I2 INT64,
			) PRIMARY KEY(T2_I1);
			CREATE TABLE T3 (
			  T3_I1 INT64 NOT NULL,
			  T3_I2 INT64,
			) PRIMARY KEY(T3_I1);
			ALTER TABLE T1 ADD CONSTRAINT FK1 FOREIGN KEY (T1_I2) REFERENCES T2 (T2_I1);
			ALTER TABLE T1 ADD FOREIGN KEY (T1_I3) REFERENCES T3 (T3_I1);
			ALTER TABLE T2 ADD CONSTRAINT FK2 FOREIGN KEY (T2_I2) REFERENCES T3 (T3_I1);
			ALTER TABLE T3 ADD CONSTRAINT C1 CHECK (T3_I2 > 0);`,
			`
			CREATE TABLE T1 (
			  T1_I1 STRING(MAX) NOT NULL,
			  T1_I2 INT64,
			  T1_I3 INT64,
			) PRIMARY KEY(T1_I1);
			CREATE TABLE T2 (
			  T2_I1 INT64 NOT NULL,
			  T2_I2 INT64,
			  T2_I3 INT64 NOT NULL,
			) PRIMARY KEY(T2_I1, T2_I3);
			CREATE TABLE T3 (
			  T3_I1 INT64 NOT NULL,
			  T3_I2 INT64,
			) PRIMARY KEY(T3_I1);
			CREATE TABLE T4 (
			  T4_I1 INT64 NOT NULL,
			  T4_S1 STRING(MAX),
			) PRIMARY KEY(T4_I1);
			ALTER TABLE T1 ADD CONSTRAINT FK1 FOREIGN KEY (T1_I2) REFERENCES T2 (T2_I1);
			ALTER TABLE T1 ADD FOREIGN KEY (T1_I3) REFERENCES T3 (T3_I1);
			ALTER TABLE T2 ADD CONSTRAINT FK2 FOREIGN KEY (T2_I2) REFERENCES T3 (T3_I1);
			ALTER TABLE T3 ADD CONSTRAINT C2 CHECK (T3_I2 > 0);
			ALTER TABLE T4 ADD FOREIGN KEY (T4_S1) REFERENCES T1 (T1_I1);`,
		},
	} {
		t.Run(name, func(t *testing.T) {
			var first string
			for i := range 50 {
				var buf bytes.Buffer
				if err := Diff(strings.NewReader(tt.base), strings.NewReader(tt.target), &buf, DiffOption{}); err != nil {
					t.Fatal(err)
				}
				if i == 0 {
					first = buf.String()
					continue
				}
				if diff := cmp.Diff(first, buf.String()); diff != "" {
					t.Fatalf("output changed between runs:\n%s", diff)
				}
			}
		})
	}
}

func TestDiffIndependentOfAlterTableOrder(t *testing.T) {
	tables := `
	CREATE TABLE T1 (
	  T1_I1 INT64 NOT NULL,
	) PRIMARY KEY(T1_I1);
	CREATE TABLE T2 (
	  T2_I1 INT64 NOT NULL,
	) PRIMARY KEY(T2_I1);
	CREATE TABLE T3 (
	  T3_I1 INT64 NOT NULL,
	) PRIMARY KEY(T3_I1);`
	fk2 := `ALTER TABLE T1 ADD FOREIGN KEY (T1_I1) REFERENCES T2 (T2_I1);`
	fk3 := `ALTER TABLE T1 ADD FOREIGN KEY (T1_I1) REFERENCES T3 (T3_I1);`

	var outputs []string
	for _, base := range []string{tables + fk2 + fk3, tables + fk3 + fk2} {
		var buf bytes.Buffer
		if err := Diff(strings.NewReader(base), strings.NewReader(""), &buf, DiffOption{}); err != nil {
			t.Fatal(err)
		}
		outputs = append(outputs, buf.String())
	}
	if diff := cmp.Diff(outputs[0], outputs[1]); diff != "" {
		t.Fatalf("output changed by the order of ALTER TABLE:\n%s", diff)
	}
}

func TestDiffErrorOnUnsupportedAlterTable(t *testing.T) {
	var buf bytes.Buffer
	err := Diff(strings.NewReader(``), strings.NewReader(`ALTER TABLE S1.S2.T1 ADD CHECK (T1_I1 > 0)`), &buf, DiffOption{
		ErrorOnUnsupportedDDL: true,
	})
	if err == nil || !strings.Contains(err.Error(), "unsupported DDL: ALTER TABLE S1.S2.T1 ADD CHECK (T1_I1 > 0)") {
		t.Fatalf("want unsupported DDL error, got %v", err)
	}
}

func TestDiffOnUnsupportedDDL(t *testing.T) {
	var ignored []string
	var buf bytes.Buffer
	target := `
	CREATE TABLE T1 (
	  T1_I1 INT64 NOT NULL,
	) PRIMARY KEY(T1_I1);
	ALTER TABLE T1 ADD CHECK (T1_I1 > 0);
	ALTER TABLE S1.S2.T1 ADD CHECK (T1_I1 > 0);
	ALTER INDEX IDX1 ADD STORED COLUMN T1_I1`
	err := Diff(strings.NewReader(``), strings.NewReader(target), &buf, DiffOption{
		OnUnsupportedDDL: func(sql string) {
			ignored = append(ignored, sql)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"ALTER TABLE S1.S2.T1 ADD CHECK (T1_I1 > 0)",
		"ALTER INDEX IDX1 ADD STORED COLUMN T1_I1",
	}
	if diff := cmp.Diff(want, ignored); diff != "" {
		t.Errorf("diff (+got -want):\n%s", diff)
	}
}
