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
		"create referenced table first": {
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
			CREATE TABLE B1 (
			  B1_I1 INT64 NOT NULL,
			) PRIMARY KEY(B1_I1);
			CREATE TABLE A1 (
			  A1_I1 INT64 NOT NULL,
			  B1_I1 INT64,
			  CONSTRAINT FK1 FOREIGN KEY (B1_I1) REFERENCES B1 (B1_I1),
			) PRIMARY KEY(A1_I1);`,
			false,
		},
		"drop referencing table first": {
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
			DROP TABLE A1;
			DROP TABLE B1;`,
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
		"error on changing identity column": {
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
		"add index": {
			``,
			`
			CREATE INDEX IDX1 ON T1(T1_S1)`,
			`
			CREATE INDEX IDX1 ON T1(T1_S1);`,
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
		"add search index": {
			``,
			`
			CREATE SEARCH INDEX IDX1 ON T1(T1_S1)`,
			`
			CREATE SEARCH INDEX IDX1 ON T1(T1_S1);`,
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
		"add property graph": {
			``,
			`
			CREATE PROPERTY GRAPH G1 NODE TABLES (T1, T2);
			`,
			`
			CREATE PROPERTY GRAPH G1 NODE TABLES (T1, T2);`,
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
		"create view": {
			``,
			`
			CREATE VIEW V1 SQL SECURITY DEFINER AS SELECT * FROM T1;`,
			`
			CREATE VIEW V1 SQL SECURITY DEFINER AS SELECT * FROM T1;`,
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
			ALTER DATABASE D2 SET OPTIONS (version_retention_period = '2d');`,
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
			CREATE VIEW V1 SQL SECURITY INVOKER AS SELECT T1.T1_I1 FROM T1;
			GRANT SELECT(T1_S1) ON TABLE T1 TO ROLE R1;`,
			`
			create table t1 (
			  t1_i1 int64 not null,
			  t1_s1 string(max) options (ALLOW_COMMIT_TIMESTAMP = false),
			  constraint c1 check (t1_i1 > 0),
			  synonym(s1),
			) primary key(t1_i1);
			create index idx1 on t1(t1_s1) storing (t1_i1);
			create view v1 sql security invoker as select t1.t1_i1 from t1;
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
	base := `
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
	CREATE INDEX IDX1 ON T1(T1_I2) STORING (T1_I3, T1_I4);`
	target := `
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
	CREATE INDEX IDX1 ON T1(T1_I2) STORING (T1_T1, T1_I1);`

	var first string
	for i := range 50 {
		var buf bytes.Buffer
		if err := Diff(strings.NewReader(base), strings.NewReader(target), &buf, DiffOption{}); err != nil {
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
}

func TestDiffOnUnsupportedDDL(t *testing.T) {
	var ignored []string
	var buf bytes.Buffer
	err := Diff(strings.NewReader(``), strings.NewReader(`ALTER INDEX IDX1 ADD STORED COLUMN T1_I1`), &buf, DiffOption{
		OnUnsupportedDDL: func(sql string) {
			ignored = append(ignored, sql)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff([]string{"ALTER INDEX IDX1 ADD STORED COLUMN T1_I1"}, ignored); diff != "" {
		t.Errorf("diff (+got -want):\n%s", diff)
	}
}
