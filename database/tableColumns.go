package database

import "fmt"

//Column struct
type Column struct {
	Column     string `db:"CL"`
	ColumnName string `db:"CN"`
	DataType   string `db:"DT"`
}

//GetColumnDetail func
func (db *Database) GetColumnDetail(dst Dbase, src Dbase, s, t string) ([]Column, error) {
	q := ""
	if src.Driver == "" {
		src = dst
	}
	if src.Driver == "mssql" {
		if dst.Driver == "mssql" {
			q += fmt.Sprintf("-- mssql to mssql\n")
			q += fmt.Sprintf("SELECT\n")
			q += fmt.Sprintf("'\"' + C.COLUMN_NAME + '\" ' +\n")
			q += fmt.Sprintf("CASE UPPER(DATA_TYPE)\n")
			q += fmt.Sprintf("WHEN 'CHAR' THEN 'CHAR' + '(' + CONVERT(VARCHAR,C.CHARACTER_MAXIMUM_LENGTH)+')'\n")
			q += fmt.Sprintf("WHEN 'NCHAR' THEN 'CHAR' + '(' + CONVERT(VARCHAR,C.CHARACTER_MAXIMUM_LENGTH)+')'\n")
			q += fmt.Sprintf("WHEN 'VARCHAR' THEN CASE WHEN C.CHARACTER_MAXIMUM_LENGTH < 0 then 'TEXT' ELSE 'VARCHAR' + '(' + CONVERT(VARCHAR,C.CHARACTER_MAXIMUM_LENGTH)+')' END\n")
			q += fmt.Sprintf("WHEN 'NVARCHAR' THEN CASE WHEN C.CHARACTER_MAXIMUM_LENGTH < 0 then 'TEXT' ELSE 'VARCHAR'+ '(' + CONVERT(VARCHAR,C.CHARACTER_MAXIMUM_LENGTH)+')' END\n")
			q += fmt.Sprintf("WHEN 'CHARACTER' THEN 'CHARACTER' + '(' + CONVERT(VARCHAR,C.CHARACTER_MAXIMUM_LENGTH)+')'\n")
			q += fmt.Sprintf("WHEN 'CHARACTER VARYING' THEN CASE WHEN C.CHARACTER_MAXIMUM_LENGTH < 0 then 'TEXT' ELSE 'CHARACTER VARYING'+ '(' + CONVERT(VARCHAR,C.CHARACTER_MAXIMUM_LENGTH)+')' END\n")
			q += fmt.Sprintf("WHEN 'TINYINT' THEN 'TINYINT'\n")
			q += fmt.Sprintf("WHEN 'SMALLINT' THEN 'SMALLINT'\n")
			q += fmt.Sprintf("WHEN 'INT' THEN 'INT'\n")
			q += fmt.Sprintf("WHEN 'DECIMAL' THEN 'DECIMAL' + '(' + CONVERT(VARCHAR,C.NUMERIC_PRECISION) + ',' + CONVERT(VARCHAR,C.NUMERIC_SCALE) + ')'\n")
			q += fmt.Sprintf("WHEN 'NUMERIC' THEN 'NUMERIC' + '(' + CONVERT(VARCHAR,C.NUMERIC_PRECISION) + ',' + CONVERT(VARCHAR,C.NUMERIC_SCALE) + ')'\n")
			q += fmt.Sprintf("WHEN 'FLOAT' THEN 'FLOAT' + CASE WHEN C.NUMERIC_PRECISION < 53 THEN '(' + CONVERT(VARCHAR,C.NUMERIC_PRECISION) + ')' ELSE '' END\n")
			q += fmt.Sprintf("WHEN 'VARBINARY' THEN 'VARBINARY'\n")
			q += fmt.Sprintf("WHEN 'DATETIME' THEN 'DATETIME'\n")
			q += fmt.Sprintf("ELSE DATA_TYPE\n")
			q += fmt.Sprintf("END + ' ' +\n")
			q += fmt.Sprintf("CASE WHEN IS_NULLABLE = 'NO' THEN 'NOT NULL' ELSE '' END + ' ' +\n")
			q += fmt.Sprintf("CASE WHEN C.COLUMN_DEFAULT IS NULL THEN ''\n")
			// q += fmt.Sprintf("ELSE ' DEFAULT ' + SUBSTRING(C.COLUMN_DEFAULT,CHARINDEX(' as ', C.COLUMN_DEFAULT)+4,LEN(C.COLUMN_DEFAULT)-CHARINDEX(' as ', C.COLUMN_DEFAULT)) END\n")
			q += fmt.Sprintf("ELSE ' DEFAULT ' + C.COLUMN_DEFAULT END\n")
			q += fmt.Sprintf("\"CL\", C.COLUMN_NAME \"CN\", UPPER(DATA_TYPE) \"DT\"\n")
			q += fmt.Sprintf("FROM %s.INFORMATION_SCHEMA.COLUMNS C\n", src.Database)
			q += fmt.Sprintf("WHERE C.TABLE_CATALOG = '%s'\n", src.Database)
			q += fmt.Sprintf("AND C.TABLE_SCHEMA = '%s'\n", s)
			q += fmt.Sprintf("AND C.TABLE_NAME = '%s'\n", t)
		} else if dst.Driver == "postgres" {
			q += fmt.Sprintf("-- mssql to pgsql\n")
			q += fmt.Sprintf("SELECT\n")
			q += fmt.Sprintf("'\"' + C.COLUMN_NAME + '\" ' +\n")
			q += fmt.Sprintf("CASE UPPER(DATA_TYPE)\n")
			q += fmt.Sprintf("WHEN 'CHAR' THEN 'CHARACTER' + '(' + CONVERT(VARCHAR,C.CHARACTER_MAXIMUM_LENGTH)+')'\n")
			q += fmt.Sprintf("WHEN 'NCHAR' THEN 'CHARACTER' + '(' + CONVERT(VARCHAR,C.CHARACTER_MAXIMUM_LENGTH)+')'\n")
			q += fmt.Sprintf("WHEN 'VARCHAR' THEN CASE WHEN C.CHARACTER_MAXIMUM_LENGTH < 0 then 'TEXT' ELSE 'CHARACTER VARYING' + '(' + CONVERT(VARCHAR,C.CHARACTER_MAXIMUM_LENGTH)+')' END\n")
			q += fmt.Sprintf("WHEN 'NVARCHAR' THEN CASE WHEN C.CHARACTER_MAXIMUM_LENGTH < 0 then 'TEXT' ELSE 'CHARACTER VARYING'+ '(' + CONVERT(VARCHAR,C.CHARACTER_MAXIMUM_LENGTH)+')' END\n")
			q += fmt.Sprintf("WHEN 'CHARACTER' THEN 'CHARACTER' + '(' + CONVERT(VARCHAR,C.CHARACTER_MAXIMUM_LENGTH) + ')'\n")
			q += fmt.Sprintf("WHEN 'CHARACTER VARYING' THEN CASE WHEN C.CHARACTER_MAXIMUM_LENGTH < 0 then 'TEXT' ELSE 'CHARACTER VARYING'+ '(' + CONVERT(VARCHAR,C.CHARACTER_MAXIMUM_LENGTH)+')' END\n")
			q += fmt.Sprintf("WHEN 'TINYINT' THEN 'SMALLINT'\n")
			q += fmt.Sprintf("WHEN 'SMALLINT' THEN 'SMALLINT'\n")
			q += fmt.Sprintf("WHEN 'INT' THEN 'INT'\n")
			q += fmt.Sprintf("WHEN 'DECIMAL' THEN 'NUMERIC' + '(' + CONVERT(VARCHAR,C.NUMERIC_PRECISION) + ',' + CONVERT(VARCHAR,C.NUMERIC_SCALE) + ')'\n")
			q += fmt.Sprintf("WHEN 'NUMERIC' THEN 'NUMERIC' + '(' + CONVERT(VARCHAR,C.NUMERIC_PRECISION) + ',' + CONVERT(VARCHAR,C.NUMERIC_SCALE) + ')'\n")
			q += fmt.Sprintf("WHEN 'FLOAT' THEN 'DOUBLE PRECISION' + CASE WHEN C.NUMERIC_PRECISION < 53 THEN '(' + CONVERT(VARCHAR,C.NUMERIC_PRECISION) + ')' ELSE '' END\n")
			q += fmt.Sprintf("WHEN 'DOUBLE PRECISION' THEN 'DOUBLE PRECISION' + CASE WHEN C.NUMERIC_PRECISION < 53 THEN '(' + CONVERT(VARCHAR,C.NUMERIC_PRECISION) + ')' ELSE '' END\n")
			q += fmt.Sprintf("WHEN 'VARBINARY' THEN 'BYTEA'\n")
			q += fmt.Sprintf("WHEN 'DATETIME' THEN 'TIMESTAMP'\n")
			q += fmt.Sprintf("ELSE DATA_TYPE\n")
			q += fmt.Sprintf("END + ' ' +\n")
			q += fmt.Sprintf("CASE WHEN IS_NULLABLE = 'NO' THEN 'NOT NULL' ELSE '' END + ' ' +\n")
			q += fmt.Sprintf("CASE WHEN C.COLUMN_DEFAULT IS NULL THEN ''\n")
			q += fmt.Sprintf("ELSE ' DEFAULT ' + substring(C.COLUMN_DEFAULT,CASE WHEN CHARINDEX(' as ', C.COLUMN_DEFAULT) = 0 then 0 else CHARINDEX(' as ', C.COLUMN_DEFAULT)+4 end,LEN(C.COLUMN_DEFAULT)+1-CASE WHEN CHARINDEX(' as ', C.COLUMN_DEFAULT) = 0 then 0 else CHARINDEX(' as ', C.COLUMN_DEFAULT) end) END\n")
			q += fmt.Sprintf("\"CL\", C.COLUMN_NAME \"CN\", UPPER(DATA_TYPE) \"DT\"\n")
			q += fmt.Sprintf("FROM %s.INFORMATION_SCHEMA.COLUMNS C\n", src.Database)
			q += fmt.Sprintf("WHERE C.TABLE_CATALOG = '%s'\n", src.Database)
			q += fmt.Sprintf("AND C.TABLE_SCHEMA = '%s'\n", s)
			q += fmt.Sprintf("AND C.TABLE_NAME = '%s'\n", t)
		}
	} else if src.Driver == "postgres" {
		if dst.Driver == "mssql" {
			q += fmt.Sprintf("-- pgsql to mssql\n")
			q += fmt.Sprintf("SELECT\n")
			q += fmt.Sprintf("'\"' || C.COLUMN_NAME || '\" ' ||\n")
			q += fmt.Sprintf("CASE UPPER(DATA_TYPE)\n")
			q += fmt.Sprintf("WHEN 'CHAR' THEN 'CHAR' || CASE WHEN C.CHARACTER_MAXIMUM_LENGTH::character varying IS NULL THEN '' ELSE '(' || C.CHARACTER_MAXIMUM_LENGTH::character varying || ')' END \n")
			q += fmt.Sprintf("WHEN 'NCHAR' THEN 'CHAR' || CASE WHEN C.CHARACTER_MAXIMUM_LENGTH::character varying IS NULL THEN '' ELSE '(' || C.CHARACTER_MAXIMUM_LENGTH::character varying || ')' END \n")
			q += fmt.Sprintf("WHEN 'VARCHAR' THEN 'VARCHAR' || CASE WHEN C.CHARACTER_MAXIMUM_LENGTH::character varying IS NULL THEN '' ELSE '(' || C.CHARACTER_MAXIMUM_LENGTH::character varying || ')' END \n")
			q += fmt.Sprintf("WHEN 'NVARCHAR' THEN 'VARCHAR' || CASE WHEN C.CHARACTER_MAXIMUM_LENGTH::character varying IS NULL THEN '' ELSE '(' || C.CHARACTER_MAXIMUM_LENGTH::character varying || ')' END \n")
			q += fmt.Sprintf("WHEN 'CHARACTER' THEN 'CHAR' || CASE WHEN C.CHARACTER_MAXIMUM_LENGTH::character varying IS NULL THEN '' ELSE '(' || C.CHARACTER_MAXIMUM_LENGTH::character varying || ')' END \n")
			q += fmt.Sprintf("WHEN 'CHARACTER VARYING' THEN 'VARCHAR' || CASE WHEN C.CHARACTER_MAXIMUM_LENGTH::character varying IS NULL THEN '' ELSE '(' || C.CHARACTER_MAXIMUM_LENGTH::character varying || ')' END \n")
			q += fmt.Sprintf("WHEN 'TINYINT' THEN 'TINYINT'\n")
			q += fmt.Sprintf("WHEN 'SMALLINT' THEN 'SMALLINT'\n")
			q += fmt.Sprintf("WHEN 'INT' THEN 'INT'\n")
			q += fmt.Sprintf("WHEN 'DECIMAL' THEN 'DECIMAL' || case when C.NUMERIC_PRECISION::character varying IS NULL THEN '' ELSE '(' || C.NUMERIC_PRECISION::character varying || ',' || C.NUMERIC_SCALE::character varying || ')' END\n")
			q += fmt.Sprintf("WHEN 'NUMERIC' THEN 'DECIMAL' || case when C.NUMERIC_PRECISION::character varying IS NULL THEN '' ELSE '(' || C.NUMERIC_PRECISION::character varying || ',' || C.NUMERIC_SCALE::character varying || ')' END\n")
			q += fmt.Sprintf("WHEN 'FLOAT' THEN 'FLOAT' || CASE WHEN C.NUMERIC_PRECISION < 53 THEN '(' || C.NUMERIC_PRECISION::character varying || ')' ELSE '' end\n")
			q += fmt.Sprintf("WHEN 'DOUBLE PRECISION' THEN 'FLOAT' || CASE WHEN C.NUMERIC_PRECISION < 53 THEN '(' || C.NUMERIC_PRECISION::character varying || ')' ELSE '' END\n")
			q += fmt.Sprintf("WHEN 'VARBINARY' THEN 'VARBINARY'\n")
			q += fmt.Sprintf("WHEN 'BYTEA' THEN 'VARBINARY'\n")
			q += fmt.Sprintf("WHEN 'DATETIME' THEN 'DATETIME'\n")
			q += fmt.Sprintf("ELSE DATA_TYPE\n")
			q += fmt.Sprintf("END || ' ' ||\n")
			q += fmt.Sprintf("CASE WHEN IS_NULLABLE = 'NO' THEN 'NOT NULL' ELSE '' END || ' ' ||\n")
			q += fmt.Sprintf("CASE WHEN C.COLUMN_DEFAULT IS NULL THEN ''\n")
			q += fmt.Sprintf("ELSE ' DEFAULT ' || case when POSITION('::' in C.COLUMN_DEFAULT) > 0 then SUBSTRING(C.COLUMN_DEFAULT,1,POSITION('::' in C.COLUMN_DEFAULT)-1) else C.COLUMN_DEFAULT END end\n")
			q += fmt.Sprintf("\"CL\", C.COLUMN_NAME \"CN\", UPPER(DATA_TYPE) \"DT\"\n")
			q += fmt.Sprintf("FROM %s.INFORMATION_SCHEMA.COLUMNS C\n", src.Database)
			q += fmt.Sprintf("WHERE C.TABLE_CATALOG = '%s'\n", src.Database)
			q += fmt.Sprintf("AND C.TABLE_SCHEMA = '%s'\n", s)
			q += fmt.Sprintf("AND C.TABLE_NAME = '%s'\n", t)
		} else if dst.Driver == "postgres" {
			q += fmt.Sprintf("-- pgsql to pgsql\n")
			q += fmt.Sprintf("SELECT\n")
			q += fmt.Sprintf("'\"' || C.COLUMN_NAME || '\" ' ||\n")
			q += fmt.Sprintf("CASE UPPER(DATA_TYPE)\n")
			q += fmt.Sprintf("WHEN 'CHAR' THEN 'CHAR' || CASE WHEN C.CHARACTER_MAXIMUM_LENGTH::character varying IS NULL THEN '' ELSE '(' || C.CHARACTER_MAXIMUM_LENGTH::character varying || ')' END \n")
			q += fmt.Sprintf("WHEN 'NCHAR' THEN 'CHAR' || CASE WHEN C.CHARACTER_MAXIMUM_LENGTH::character varying IS NULL THEN '' ELSE '(' || C.CHARACTER_MAXIMUM_LENGTH::character varying || ')' END \n")
			q += fmt.Sprintf("WHEN 'VARCHAR' THEN 'VARCHAR' || CASE WHEN C.CHARACTER_MAXIMUM_LENGTH::character varying IS NULL THEN '' ELSE '(' || C.CHARACTER_MAXIMUM_LENGTH::character varying || ')' END \n")
			q += fmt.Sprintf("WHEN 'NVARCHAR' THEN 'VARCHAR' || CASE WHEN C.CHARACTER_MAXIMUM_LENGTH::character varying IS NULL THEN '' ELSE '(' || C.CHARACTER_MAXIMUM_LENGTH::character varying || ')' END \n")
			q += fmt.Sprintf("WHEN 'CHARACTER' THEN 'CHARACTER' || CASE WHEN C.CHARACTER_MAXIMUM_LENGTH::character varying IS NULL THEN '' ELSE '(' || C.CHARACTER_MAXIMUM_LENGTH::character varying || ')' END \n")
			q += fmt.Sprintf("WHEN 'CHARACTER VARYING' THEN 'CHARACTER VARYING' || CASE WHEN C.CHARACTER_MAXIMUM_LENGTH::character varying IS NULL THEN '' ELSE '(' || C.CHARACTER_MAXIMUM_LENGTH::character varying || ')' END \n")
			q += fmt.Sprintf("WHEN 'TINYINT' THEN 'TINYINT'\n")
			q += fmt.Sprintf("WHEN 'SMALLINT' THEN 'SMALLINT'\n")
			q += fmt.Sprintf("WHEN 'INT' THEN 'INT'\n")
			q += fmt.Sprintf("WHEN 'DECIMAL' THEN 'DECIMAL' || case when C.NUMERIC_PRECISION::character varying IS NULL THEN '' ELSE '(' || C.NUMERIC_PRECISION::character varying || ',' || C.NUMERIC_SCALE::character varying || ')' END\n")
			q += fmt.Sprintf("WHEN 'NUMERIC' THEN 'NUMERIC' || case when C.NUMERIC_PRECISION::character varying IS NULL THEN '' ELSE '(' || C.NUMERIC_PRECISION::character varying || ',' || C.NUMERIC_SCALE::character varying || ')' END\n")
			q += fmt.Sprintf("WHEN 'FLOAT' THEN 'FLOAT' || CASE WHEN C.NUMERIC_PRECISION < 53 THEN '(' || C.NUMERIC_PRECISION::character varying || ')' ELSE '' END\n")
			q += fmt.Sprintf("WHEN 'VARBINARY' THEN 'VARBINARY'\n")
			q += fmt.Sprintf("WHEN 'DATETIME' THEN 'DATETIME'\n")
			q += fmt.Sprintf("ELSE DATA_TYPE\n")
			q += fmt.Sprintf("END || ' ' ||\n")
			q += fmt.Sprintf("CASE WHEN IS_NULLABLE = 'NO' THEN 'NOT NULL' ELSE '' END || ' ' ||\n")
			q += fmt.Sprintf("CASE WHEN C.COLUMN_DEFAULT IS NULL THEN ''\n")
			q += fmt.Sprintf("ELSE ' DEFAULT ' || C.COLUMN_DEFAULT END\n")
			q += fmt.Sprintf("\"CL\", C.COLUMN_NAME \"CN\", UPPER(DATA_TYPE) \"DT\"\n")
			q += fmt.Sprintf("FROM %s.INFORMATION_SCHEMA.COLUMNS C\n", src.Database)
			q += fmt.Sprintf("WHERE C.TABLE_CATALOG = '%s'\n", src.Database)
			q += fmt.Sprintf("AND C.TABLE_SCHEMA = '%s'\n", s)
			q += fmt.Sprintf("AND C.TABLE_NAME = '%s'\n", t)
		}
	}

	// fmt.Println(q)
	columnnames := []Column{}
	if err := db.Select(&columnnames, q); err != nil {
		return nil, fmt.Errorf("Select: %v", err)
	}
	return columnnames, nil
}
