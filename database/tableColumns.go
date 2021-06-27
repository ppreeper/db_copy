package database

import "fmt"

//Column struct
type Column struct {
	Column     string `db:"CL"`
	ColumnName string `db:"CN"`
	DataType   string `db:"DT"`
}

//GetColumnDetail func
func (db *Database) GetColumnDetail(conn *Conn, t string, debug bool) ([]Column, error) {
	q := ""
	if conn.Source.Driver == "mssql" {
		if conn.Dest.Driver == "mssql" {
			q += "-- mssql to mssql\n"
			q += "SELECT\n"
			q += "'\"' + C.COLUMN_NAME + '\" ' +\n"
			q += "CASE UPPER(DATA_TYPE)\n"
			q += "WHEN 'CHAR' THEN 'CHAR' + '(' + CONVERT(VARCHAR,C.CHARACTER_MAXIMUM_LENGTH)+')'\n"
			q += "WHEN 'NCHAR' THEN 'CHAR' + '(' + CONVERT(VARCHAR,C.CHARACTER_MAXIMUM_LENGTH)+')'\n"
			q += "WHEN 'VARCHAR' THEN CASE WHEN C.CHARACTER_MAXIMUM_LENGTH < 0 then 'TEXT' ELSE 'VARCHAR' + '(' + CONVERT(VARCHAR,C.CHARACTER_MAXIMUM_LENGTH)+')' END\n"
			q += "WHEN 'NVARCHAR' THEN CASE WHEN C.CHARACTER_MAXIMUM_LENGTH < 0 then 'TEXT' ELSE 'VARCHAR'+ '(' + CONVERT(VARCHAR,C.CHARACTER_MAXIMUM_LENGTH)+')' END\n"
			q += "WHEN 'CHARACTER' THEN 'CHARACTER' + '(' + CONVERT(VARCHAR,C.CHARACTER_MAXIMUM_LENGTH)+')'\n"
			q += "WHEN 'CHARACTER VARYING' THEN CASE WHEN C.CHARACTER_MAXIMUM_LENGTH < 0 then 'TEXT' ELSE 'CHARACTER VARYING'+ '(' + CONVERT(VARCHAR,C.CHARACTER_MAXIMUM_LENGTH)+')' END\n"
			q += "WHEN 'TINYINT' THEN 'TINYINT'\n"
			q += "WHEN 'SMALLINT' THEN 'SMALLINT'\n"
			q += "WHEN 'INT' THEN 'INT'\n"
			q += "WHEN 'DECIMAL' THEN 'DECIMAL' + '(' + CONVERT(VARCHAR,C.NUMERIC_PRECISION) + ',' + CONVERT(VARCHAR,C.NUMERIC_SCALE) + ')'\n"
			q += "WHEN 'NUMERIC' THEN 'NUMERIC' + '(' + CONVERT(VARCHAR,C.NUMERIC_PRECISION) + ',' + CONVERT(VARCHAR,C.NUMERIC_SCALE) + ')'\n"
			q += "WHEN 'FLOAT' THEN 'FLOAT' + CASE WHEN C.NUMERIC_PRECISION < 53 THEN '(' + CONVERT(VARCHAR,C.NUMERIC_PRECISION) + ')' ELSE '' END\n"
			q += "WHEN 'VARBINARY' THEN 'VARBINARY'\n"
			q += "WHEN 'DATETIME' THEN 'DATETIME'\n"
			q += "ELSE DATA_TYPE\n"
			q += "END + ' ' +\n"
			q += "CASE WHEN IS_NULLABLE = 'NO' THEN 'NOT NULL' ELSE '' END + ' ' +\n"
			q += "CASE WHEN C.COLUMN_DEFAULT IS NULL THEN ''\n"
			q += "ELSE ' DEFAULT ' + SUBSTRING(C.COLUMN_DEFAULT,CHARINDEX(' as ', C.COLUMN_DEFAULT)+4,LEN(C.COLUMN_DEFAULT)-CHARINDEX(' as ', C.COLUMN_DEFAULT)) END\n"
			q += "\"CL\", C.COLUMN_NAME \"CN\", UPPER(DATA_TYPE) \"DT\"\n"
		} else if conn.Dest.Driver == "postgres" {
			q += "-- mssql to pgsql\n"
			q += "SELECT\n"
			q += "'\"' + C.COLUMN_NAME + '\" ' +\n"
			q += "CASE UPPER(DATA_TYPE)\n"
			q += "WHEN 'CHAR' THEN 'CHARACTER' + '(' + CONVERT(VARCHAR,C.CHARACTER_MAXIMUM_LENGTH)+')'\n"
			q += "WHEN 'NCHAR' THEN 'CHARACTER' + '(' + CONVERT(VARCHAR,C.CHARACTER_MAXIMUM_LENGTH)+')'\n"
			q += "WHEN 'VARCHAR' THEN CASE WHEN C.CHARACTER_MAXIMUM_LENGTH < 0 then 'TEXT' ELSE 'CHARACTER VARYING' + '(' + CONVERT(VARCHAR,C.CHARACTER_MAXIMUM_LENGTH)+')' END\n"
			q += "WHEN 'NVARCHAR' THEN CASE WHEN C.CHARACTER_MAXIMUM_LENGTH < 0 then 'TEXT' ELSE 'CHARACTER VARYING'+ '(' + CONVERT(VARCHAR,C.CHARACTER_MAXIMUM_LENGTH)+')' END\n"
			q += "WHEN 'CHARACTER' THEN 'CHARACTER' + '(' + CONVERT(VARCHAR,C.CHARACTER_MAXIMUM_LENGTH) + ')'\n"
			q += "WHEN 'CHARACTER VARYING' THEN CASE WHEN C.CHARACTER_MAXIMUM_LENGTH < 0 then 'TEXT' ELSE 'CHARACTER VARYING'+ '(' + CONVERT(VARCHAR,C.CHARACTER_MAXIMUM_LENGTH)+')' END\n"
			q += "WHEN 'TINYINT' THEN 'SMALLINT'\n"
			q += "WHEN 'SMALLINT' THEN 'SMALLINT'\n"
			q += "WHEN 'INT' THEN 'INT'\n"
			q += "WHEN 'DECIMAL' THEN 'NUMERIC' + '(' + CONVERT(VARCHAR,C.NUMERIC_PRECISION) + ',' + CONVERT(VARCHAR,C.NUMERIC_SCALE) + ')'\n"
			q += "WHEN 'NUMERIC' THEN 'NUMERIC' + '(' + CONVERT(VARCHAR,C.NUMERIC_PRECISION) + ',' + CONVERT(VARCHAR,C.NUMERIC_SCALE) + ')'\n"
			q += "WHEN 'FLOAT' THEN 'DOUBLE PRECISION' + CASE WHEN C.NUMERIC_PRECISION < 53 THEN '(' + CONVERT(VARCHAR,C.NUMERIC_PRECISION) + ')' ELSE '' END\n"
			q += "WHEN 'DOUBLE PRECISION' THEN 'DOUBLE PRECISION' + CASE WHEN C.NUMERIC_PRECISION < 53 THEN '(' + CONVERT(VARCHAR,C.NUMERIC_PRECISION) + ')' ELSE '' END\n"
			q += "WHEN 'VARBINARY' THEN 'BYTEA'\n"
			q += "WHEN 'DATETIME' THEN 'TIMESTAMP'\n"
			q += "ELSE DATA_TYPE\n"
			q += "END + ' ' +\n"
			q += "CASE WHEN IS_NULLABLE = 'NO' THEN 'NOT NULL' ELSE '' END + ' ' +\n"
			q += "CASE WHEN C.COLUMN_DEFAULT IS NULL THEN ''\n"
			q += "ELSE ' DEFAULT ' + substring(C.COLUMN_DEFAULT,CASE WHEN CHARINDEX(' as ', C.COLUMN_DEFAULT) = 0 then 0 else CHARINDEX(' as ', C.COLUMN_DEFAULT)+4 end,LEN(C.COLUMN_DEFAULT)+1-CASE WHEN CHARINDEX(' as ', C.COLUMN_DEFAULT) = 0 then 0 else CHARINDEX(' as ', C.COLUMN_DEFAULT) end) END\n"
			q += "\"CL\", C.COLUMN_NAME \"CN\", UPPER(DATA_TYPE) \"DT\"\n"
		}
	} else if conn.Source.Driver == "postgres" {
		if conn.Dest.Driver == "mssql" {
			q += "-- pgsql to mssql\n"
			q += "SELECT\n"
			q += "'\"' || C.COLUMN_NAME || '\" ' ||\n"
			q += "CASE UPPER(DATA_TYPE)\n"
			q += "WHEN 'CHAR' THEN 'CHAR' || CASE WHEN C.CHARACTER_MAXIMUM_LENGTH::character varying IS NULL THEN '' ELSE '(' || C.CHARACTER_MAXIMUM_LENGTH::character varying || ')' END \n"
			q += "WHEN 'NCHAR' THEN 'CHAR' || CASE WHEN C.CHARACTER_MAXIMUM_LENGTH::character varying IS NULL THEN '' ELSE '(' || C.CHARACTER_MAXIMUM_LENGTH::character varying || ')' END \n"
			q += "WHEN 'VARCHAR' THEN 'VARCHAR' || CASE WHEN C.CHARACTER_MAXIMUM_LENGTH::character varying IS NULL THEN '' ELSE '(' || C.CHARACTER_MAXIMUM_LENGTH::character varying || ')' END \n"
			q += "WHEN 'NVARCHAR' THEN 'VARCHAR' || CASE WHEN C.CHARACTER_MAXIMUM_LENGTH::character varying IS NULL THEN '' ELSE '(' || C.CHARACTER_MAXIMUM_LENGTH::character varying || ')' END \n"
			q += "WHEN 'CHARACTER' THEN 'CHAR' || CASE WHEN C.CHARACTER_MAXIMUM_LENGTH::character varying IS NULL THEN '' ELSE '(' || C.CHARACTER_MAXIMUM_LENGTH::character varying || ')' END \n"
			q += "WHEN 'CHARACTER VARYING' THEN 'VARCHAR' || CASE WHEN C.CHARACTER_MAXIMUM_LENGTH::character varying IS NULL THEN '' ELSE '(' || C.CHARACTER_MAXIMUM_LENGTH::character varying || ')' END \n"
			q += "WHEN 'TINYINT' THEN 'TINYINT'\n"
			q += "WHEN 'SMALLINT' THEN 'SMALLINT'\n"
			q += "WHEN 'INT' THEN 'INT'\n"
			q += "WHEN 'DECIMAL' THEN 'DECIMAL' || case when C.NUMERIC_PRECISION::character varying IS NULL THEN '' ELSE '(' || C.NUMERIC_PRECISION::character varying || ',' || C.NUMERIC_SCALE::character varying || ')' END\n"
			q += "WHEN 'NUMERIC' THEN 'DECIMAL' || case when C.NUMERIC_PRECISION::character varying IS NULL THEN '' ELSE '(' || C.NUMERIC_PRECISION::character varying || ',' || C.NUMERIC_SCALE::character varying || ')' END\n"
			q += "WHEN 'FLOAT' THEN 'FLOAT' || CASE WHEN C.NUMERIC_PRECISION < 53 THEN '(' || C.NUMERIC_PRECISION::character varying || ')' ELSE '' end\n"
			q += "WHEN 'DOUBLE PRECISION' THEN 'FLOAT' || CASE WHEN C.NUMERIC_PRECISION < 53 THEN '(' || C.NUMERIC_PRECISION::character varying || ')' ELSE '' END\n"
			q += "WHEN 'VARBINARY' THEN 'VARBINARY'\n"
			q += "WHEN 'BYTEA' THEN 'VARBINARY'\n"
			q += "WHEN 'DATETIME' THEN 'DATETIME'\n"
			q += "ELSE DATA_TYPE\n"
			q += "END || ' ' ||\n"
			q += "CASE WHEN IS_NULLABLE = 'NO' THEN 'NOT NULL' ELSE '' END || ' ' ||\n"
			q += "CASE WHEN C.COLUMN_DEFAULT IS NULL THEN ''\n"
			q += "ELSE ' DEFAULT ' || case when POSITION('::' in C.COLUMN_DEFAULT) > 0 then SUBSTRING(C.COLUMN_DEFAULT,1,POSITION('::' in C.COLUMN_DEFAULT)-1) else C.COLUMN_DEFAULT END end\n"
			q += "\"CL\", C.COLUMN_NAME \"CN\", UPPER(DATA_TYPE) \"DT\"\n"
		} else if conn.Dest.Driver == "postgres" {
			q += "-- pgsql to pgsql\n"
			q += "SELECT\n"
			q += "'\"' || C.COLUMN_NAME || '\" ' ||\n"
			q += "CASE UPPER(DATA_TYPE)\n"
			q += "WHEN 'CHAR' THEN 'CHAR' || CASE WHEN C.CHARACTER_MAXIMUM_LENGTH::character varying IS NULL THEN '' ELSE '(' || C.CHARACTER_MAXIMUM_LENGTH::character varying || ')' END \n"
			q += "WHEN 'NCHAR' THEN 'CHAR' || CASE WHEN C.CHARACTER_MAXIMUM_LENGTH::character varying IS NULL THEN '' ELSE '(' || C.CHARACTER_MAXIMUM_LENGTH::character varying || ')' END \n"
			q += "WHEN 'VARCHAR' THEN 'VARCHAR' || CASE WHEN C.CHARACTER_MAXIMUM_LENGTH::character varying IS NULL THEN '' ELSE '(' || C.CHARACTER_MAXIMUM_LENGTH::character varying || ')' END \n"
			q += "WHEN 'NVARCHAR' THEN 'VARCHAR' || CASE WHEN C.CHARACTER_MAXIMUM_LENGTH::character varying IS NULL THEN '' ELSE '(' || C.CHARACTER_MAXIMUM_LENGTH::character varying || ')' END \n"
			q += "WHEN 'CHARACTER' THEN 'CHARACTER' || CASE WHEN C.CHARACTER_MAXIMUM_LENGTH::character varying IS NULL THEN '' ELSE '(' || C.CHARACTER_MAXIMUM_LENGTH::character varying || ')' END \n"
			q += "WHEN 'CHARACTER VARYING' THEN 'CHARACTER VARYING' || CASE WHEN C.CHARACTER_MAXIMUM_LENGTH::character varying IS NULL THEN '' ELSE '(' || C.CHARACTER_MAXIMUM_LENGTH::character varying || ')' END \n"
			q += "WHEN 'TINYINT' THEN 'TINYINT'\n"
			q += "WHEN 'SMALLINT' THEN 'SMALLINT'\n"
			q += "WHEN 'INT' THEN 'INT'\n"
			q += "WHEN 'DECIMAL' THEN 'DECIMAL' || case when C.NUMERIC_PRECISION::character varying IS NULL THEN '' ELSE '(' || C.NUMERIC_PRECISION::character varying || ',' || C.NUMERIC_SCALE::character varying || ')' END\n"
			q += "WHEN 'NUMERIC' THEN 'NUMERIC' || case when C.NUMERIC_PRECISION::character varying IS NULL THEN '' ELSE '(' || C.NUMERIC_PRECISION::character varying || ',' || C.NUMERIC_SCALE::character varying || ')' END\n"
			q += "WHEN 'FLOAT' THEN 'FLOAT' || CASE WHEN C.NUMERIC_PRECISION < 53 THEN '(' || C.NUMERIC_PRECISION::character varying || ')' ELSE '' END\n"
			q += "WHEN 'VARBINARY' THEN 'VARBINARY'\n"
			q += "WHEN 'DATETIME' THEN 'DATETIME'\n"
			q += "ELSE DATA_TYPE\n"
			q += "END || ' ' ||\n"
			q += "CASE WHEN IS_NULLABLE = 'NO' THEN 'NOT NULL' ELSE '' END || ' ' ||\n"
			q += "CASE WHEN C.COLUMN_DEFAULT IS NULL THEN ''\n"
			q += "ELSE ' DEFAULT ' || C.COLUMN_DEFAULT END\n"
			q += "\"CL\", C.COLUMN_NAME \"CN\", UPPER(DATA_TYPE) \"DT\"\n"
		}
	}

	q += fmt.Sprintf("FROM %s.INFORMATION_SCHEMA.COLUMNS C\n", conn.Source.Database)
	q += fmt.Sprintf("WHERE C.TABLE_CATALOG = '%s'\n", conn.Source.Database)
	q += fmt.Sprintf("AND C.TABLE_SCHEMA = '%s'\n", conn.SSchema)
	q += fmt.Sprintf("AND C.TABLE_NAME = '%s'\n", t)
	q += "ORDER BY ORDINAL_POSITION;\n;"

	if debug {
		fmt.Println(q)
	}
	columnnames := []Column{}
	if err := db.Select(&columnnames, q); err != nil {
		return nil, fmt.Errorf("select: %v", err)
	}
	return columnnames, nil
}
