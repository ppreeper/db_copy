package main

import (
	"fmt"
	"log"
	"strings"

	_ "github.com/denisenkom/go-mssqldb" //mssql driver
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq" //postgresql driver
	// _ "github.com/denisenkom/go-mssqldb"
	// _ "github.com/fajran/go-monetdb" //Monet
	// _ "github.com/mattn/go-sqlite3" //sqlite3
	// _ "gopkg.in/mgo.v2" //Mongo
	// _ "github.com/go-sql-driver/mysql/" //MySql
	// _ "github.com/nakagami/firebirdsql" //Firebird Sql
	// _ "bitbucket.org/phiggins/db2cli" //DB2
)

// Database struct contains sql pointer
type Database struct {
	*sqlx.DB
}

// Dbase for loading from json
type Dbase struct {
	Name     string `json:"name"`
	Driver   string `json:"driver"`
	Host     string `json:"host"`
	Port     string `json:"port"`
	Database string `json:"database"`
	Schema   string `json:"schema"`
	Username string `json:"username"`
	Password string `json:"password"`
}

// OpenDatabase open database
func OpenDatabase(driver string, dburi string) (*Database, error) {
	// fmt.Println(driver, dburi)
	var err error
	db := Database{}
	db.DB, err = sqlx.Open(driver, dburi)
	if err != nil {
		log.Printf("Open sql (%v): %v", dburi, err)
	}
	if err = db.Ping(); err != nil {
		log.Printf("Ping sql: %v", err)
	}
	return &db, err
}

//TableNames list of tables
type TableNames struct {
	TableName string `db:"TABLE_NAME"`
}

// GetTables returns table list
func (db *Database) GetTables(d Dbase, schemaName string) ([]TableNames, error) {
	q := ""
	q += fmt.Sprintf("SELECT TABLE_NAME \n")
	q += fmt.Sprintf("FROM INFORMATION_SCHEMA.TABLES\n")
	q += fmt.Sprintf("WHERE TABLE_CATALOG = '%s' AND TABLE_SCHEMA = '%s'\n", d.Database, schemaName)
	q += fmt.Sprintf("AND TABLE_TYPE = 'BASE TABLE'\n")
	q += fmt.Sprintf("ORDER BY TABLE_NAME")
	tablenames := []TableNames{}
	if err := db.Select(&tablenames, q); err != nil {
		return nil, fmt.Errorf("Select: %v", err)
	}
	return tablenames, nil
}

//ExecProcedure executes stored procedure
func (db *Database) ExecProcedure(q string) {
	fmt.Println(q)
	_, err := db.Exec(q)
	if err != nil {
		panic(err)
	}
}

//Columns struct
type Columns struct {
	Column     string `db:"CL"`
	ColumnName string `db:"CN"`
	DataType   string `db:"DT"`
}

//GetColumnDetail func
func (db *Database) GetColumnDetail(dst Dbase, src Dbase, s, t string) ([]Columns, error) {
	q := ""
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
			q += fmt.Sprintf("ELSE ' DEFAULT ' + SUBSTRING(C.COLUMN_DEFAULT,CHARINDEX(' as ', C.COLUMN_DEFAULT)+4,LEN(C.COLUMN_DEFAULT)-CHARINDEX(' as ', C.COLUMN_DEFAULT)) END\n")
			q += fmt.Sprintf("CL, C.COLUMN_NAME CN, UPPER(DATA_TYPE) DT\n")
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
			q += fmt.Sprintf("CL, C.COLUMN_NAME CN, UPPER(DATA_TYPE) DT\n")
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
			q += fmt.Sprintf("WHEN 'CHAR' THEN 'CHAR' || '(' || C.CHARACTER_MAXIMUM_LENGTH::character varying || ')'\n")
			q += fmt.Sprintf("WHEN 'NCHAR' THEN 'CHAR' || '(' || C.CHARACTER_MAXIMUM_LENGTH::character varying || ')'\n")
			q += fmt.Sprintf("WHEN 'VARCHAR' THEN 'VARCHAR' || '(' || C.CHARACTER_MAXIMUM_LENGTH::character varying || ')'\n")
			q += fmt.Sprintf("WHEN 'NVARCHAR' THEN 'VARCHAR' || '(' || C.CHARACTER_MAXIMUM_LENGTH::character varying || ')'\n")
			q += fmt.Sprintf("WHEN 'CHARACTER' THEN 'CHAR' || '(' || C.CHARACTER_MAXIMUM_LENGTH::character varying || ')'\n")
			q += fmt.Sprintf("WHEN 'CHARACTER VARYING' THEN 'VARCHAR' || '(' || C.CHARACTER_MAXIMUM_LENGTH::character varying || ')'\n")
			q += fmt.Sprintf("WHEN 'TINYINT' THEN 'TINYINT'\n")
			q += fmt.Sprintf("WHEN 'SMALLINT' THEN 'SMALLINT'\n")
			q += fmt.Sprintf("WHEN 'INT' THEN 'INT'\n")
			q += fmt.Sprintf("WHEN 'DECIMAL' THEN 'DECIMAL' || '(' || C.NUMERIC_PRECISION::character varying || ',' || C.NUMERIC_SCALE::character varying || ')'\n")
			q += fmt.Sprintf("WHEN 'NUMERIC' THEN 'DECIMAL' || '(' || C.NUMERIC_PRECISION::character varying || ',' || C.NUMERIC_SCALE::character varying || ')'\n")
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
			q += fmt.Sprintf("CL, C.COLUMN_NAME CN, UPPER(DATA_TYPE) DT\n")
			q += fmt.Sprintf("FROM %s.INFORMATION_SCHEMA.COLUMNS C\n", src.Database)
			q += fmt.Sprintf("WHERE C.TABLE_CATALOG = '%s'\n", src.Database)
			q += fmt.Sprintf("AND C.TABLE_SCHEMA = '%s'\n", s)
			q += fmt.Sprintf("AND C.TABLE_NAME = '%s'\n", t)
		} else if dst.Driver == "postgres" {
			q += fmt.Sprintf("-- pgsql to pgsql\n")
			q += fmt.Sprintf("SELECT\n")
			q += fmt.Sprintf("'\"' || C.COLUMN_NAME || '\" ' ||\n")
			q += fmt.Sprintf("CASE UPPER(DATA_TYPE)\n")
			q += fmt.Sprintf("WHEN 'CHAR' THEN 'CHAR' || '(' || C.CHARACTER_MAXIMUM_LENGTH::character varying || ')'\n")
			q += fmt.Sprintf("WHEN 'NCHAR' THEN 'CHAR' || '(' || C.CHARACTER_MAXIMUM_LENGTH::character varying || ')'\n")
			q += fmt.Sprintf("WHEN 'VARCHAR' THEN 'VARCHAR' || '(' || C.CHARACTER_MAXIMUM_LENGTH::character varying || ')'\n")
			q += fmt.Sprintf("WHEN 'NVARCHAR' THEN 'VARCHAR' || '(' || C.CHARACTER_MAXIMUM_LENGTH::character varying || ')'\n")
			q += fmt.Sprintf("WHEN 'CHARACTER' THEN 'CHARACTER' || '(' || C.CHARACTER_MAXIMUM_LENGTH::character varying || ')'\n")
			q += fmt.Sprintf("WHEN 'CHARACTER VARYING' THEN 'CHARACTER VARYING' || '(' || C.CHARACTER_MAXIMUM_LENGTH::character varying || ')'\n")
			q += fmt.Sprintf("WHEN 'TINYINT' THEN 'TINYINT'\n")
			q += fmt.Sprintf("WHEN 'SMALLINT' THEN 'SMALLINT'\n")
			q += fmt.Sprintf("WHEN 'INT' THEN 'INT'\n")
			q += fmt.Sprintf("WHEN 'DECIMAL' THEN 'DECIMAL' || '(' || C.NUMERIC_PRECISION::character varying || ',' || C.NUMERIC_SCALE::character varying || ')'\n")
			q += fmt.Sprintf("WHEN 'NUMERIC' THEN 'NUMERIC' || '(' || C.NUMERIC_PRECISION::character varying || ',' || C.NUMERIC_SCALE::character varying || ')'\n")
			q += fmt.Sprintf("WHEN 'FLOAT' THEN 'FLOAT' || CASE WHEN C.NUMERIC_PRECISION < 53 THEN '(' || C.NUMERIC_PRECISION::character varying || ')' ELSE '' END\n")
			q += fmt.Sprintf("WHEN 'VARBINARY' THEN 'VARBINARY'\n")
			q += fmt.Sprintf("WHEN 'DATETIME' THEN 'DATETIME'\n")
			q += fmt.Sprintf("ELSE DATA_TYPE\n")
			q += fmt.Sprintf("END || ' ' ||\n")
			q += fmt.Sprintf("CASE WHEN IS_NULLABLE = 'NO' THEN 'NOT NULL' ELSE '' END || ' ' ||\n")
			q += fmt.Sprintf("CASE WHEN C.COLUMN_DEFAULT IS NULL THEN ''\n")
			q += fmt.Sprintf("ELSE ' DEFAULT ' || C.COLUMN_DEFAULT END\n")
			q += fmt.Sprintf("CL, C.COLUMN_NAME CN, UPPER(DATA_TYPE) DT\n")
			q += fmt.Sprintf("FROM %s.INFORMATION_SCHEMA.COLUMNS C\n", src.Database)
			q += fmt.Sprintf("WHERE C.TABLE_CATALOG = '%s'\n", src.Database)
			q += fmt.Sprintf("AND C.TABLE_SCHEMA = '%s'\n", s)
			q += fmt.Sprintf("AND C.TABLE_NAME = '%s'\n", t)
		}
	}
	// fmt.Println(q)
	columnnames := []Columns{}
	if err := db.Select(&columnnames, q); err != nil {
		return nil, fmt.Errorf("Select: %v", err)
	}
	return columnnames, nil
}

//PKey struct
type PKey struct {
	PKey string `db:"CL"`
}

//GetPKey func
func (db *Database) GetPKey(dst Dbase, src Dbase, s, t string) ([]PKey, error) {
	q := ""
	if src.Driver == "postgres" {
		q += fmt.Sprintf("SELECT C.COLUMN_NAME CL")
		q += fmt.Sprintf("\nFROM %s.INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE C", src.Database)
		q += fmt.Sprintf("\nJOIN %s.INFORMATION_SCHEMA.COLUMNS CLM ON", src.Database)
		q += fmt.Sprintf("\nC.TABLE_CATALOG = CLM.TABLE_CATALOG AND ")
		q += fmt.Sprintf("\nC.TABLE_SCHEMA = CLM.TABLE_SCHEMA AND ")
		q += fmt.Sprintf("\nC.TABLE_NAME = CLM.TABLE_NAME AND ")
		q += fmt.Sprintf("\nC.COLUMN_NAME = CLM.COLUMN_NAME")
		q += fmt.Sprintf("\nWHERE C.TABLE_CATALOG = '%s'", src.Database)
		q += fmt.Sprintf("\nAND C.TABLE_SCHEMA = '%s'", s)
		q += fmt.Sprintf("\nAND C.TABLE_NAME IN ('%s')", t)
		q += fmt.Sprintf("\nAND C.CONSTRAINT_NAME IN (")
		q += fmt.Sprintf("\nSELECT CONSTRAINT_NAME")
		q += fmt.Sprintf("\nFROM %s.INFORMATION_SCHEMA.TABLE_CONSTRAINTS C", src.Database)
		q += fmt.Sprintf("\nWHERE C.TABLE_CATALOG = '%s'", src.Database)
		q += fmt.Sprintf("\nAND C.TABLE_SCHEMA = '%s'", s)
		q += fmt.Sprintf("\nAND CONSTRAINT_TYPE = 'PRIMARY KEY'")
		q += fmt.Sprintf("\nAND C.TABLE_NAME IN ('%s')", t)
		q += fmt.Sprintf("\n)")
		q += fmt.Sprintf("\nORDER BY CLM.ORDINAL_POSITION")
	} else if src.Driver == "mssql" {
		q += fmt.Sprintf("SELECT C.COLUMN_NAME CL")
		q += fmt.Sprintf("\nFROM %s.INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE C", src.Database)
		q += fmt.Sprintf("\nJOIN %s.INFORMATION_SCHEMA.COLUMNS CLM ON", src.Database)
		q += fmt.Sprintf("\nC.TABLE_CATALOG = CLM.TABLE_CATALOG AND ")
		q += fmt.Sprintf("\nC.TABLE_SCHEMA = CLM.TABLE_SCHEMA AND ")
		q += fmt.Sprintf("\nC.TABLE_NAME = CLM.TABLE_NAME AND ")
		q += fmt.Sprintf("\nC.COLUMN_NAME = CLM.COLUMN_NAME")
		q += fmt.Sprintf("\nWHERE C.TABLE_CATALOG = '%s'", src.Database)
		q += fmt.Sprintf("\nAND C.TABLE_SCHEMA = '%s'", s)
		q += fmt.Sprintf("\nAND C.TABLE_NAME IN ('%s')", t)
		q += fmt.Sprintf("\nAND C.CONSTRAINT_NAME IN (")
		q += fmt.Sprintf("\nSELECT CONSTRAINT_NAME")
		q += fmt.Sprintf("\nFROM %s.INFORMATION_SCHEMA.TABLE_CONSTRAINTS C", src.Database)
		q += fmt.Sprintf("\nWHERE C.TABLE_CATALOG = '%s'", src.Database)
		q += fmt.Sprintf("\nAND C.TABLE_SCHEMA = '%s'", s)
		q += fmt.Sprintf("\nAND CONSTRAINT_TYPE = 'PRIMARY KEY'")
		q += fmt.Sprintf("\nAND C.TABLE_NAME IN ('%s')", t)
		q += fmt.Sprintf("\n)")
		q += fmt.Sprintf("\nORDER BY CLM.ORDINAL_POSITION")
	}
	// fmt.Println(q)
	pkey := []PKey{}
	if err := db.Select(&pkey, q); err != nil {
		return nil, fmt.Errorf("Select: %v", err)
	}
	return pkey, nil
}

//GenTable generate table craeation
func (db *Database) GenTable(dst Dbase, s, t string, cols []Columns, pkey []PKey) (sqld, sqlc string) {
	// fmt.Println(dst.Driver, s, t, pkey)
	clen := len(cols)
	plen := len(pkey)
	if dst.Driver == "postgres" {
		sqld += fmt.Sprintf("DROP TABLE IF EXISTS \"%s\".\"%s\" CASCADE;\n", s, t)
		sqlc += fmt.Sprintf("CREATE TABLE IF NOT EXISTS \"%s\".\"%s\" (\n", s, t)
		for k, c := range cols {
			if k == clen-1 {
				if plen > 0 {
					sqlc += c.Column + ",\n"
					sqlc += "PRIMARY KEY ("
					for v, p := range pkey {
						if v == plen-1 {
							sqlc += "\"" + p.PKey + "\""
						} else {
							sqlc += "\"" + p.PKey + "\","
						}
					}
					sqlc += ")" + "\n"
				} else {
					sqlc += c.Column + "\n"
				}
			} else {
				sqlc += c.Column + ",\n"
			}
		}
		sqlc += ")" + ";\n"
	} else if dst.Driver == "mssql" {
		sqld += fmt.Sprintf("DROP TABLE \"%s\".\"%s\";\n", s, t)
		sqlc += fmt.Sprintf("CREATE TABLE %s.%s (\n", s, t)
		for k, c := range cols {
			if k == clen-1 {
				if plen > 0 {
					sqlc += fmt.Sprintf("%s,\n", c.Column)
					sqlc += fmt.Sprintf("PRIMARY KEY (")
					for v, p := range pkey {
						if v == plen-1 {
							sqlc += fmt.Sprintf("\"%s\"", p.PKey)
						} else {
							sqlc += fmt.Sprintf("\"%s\",", p.PKey)
						}
					}
					sqlc += fmt.Sprintf(")\n")
				} else {
					// q += c.Column + "\n"
					sqlc += fmt.Sprintf("%s\n", c.Column)
				}
			} else {
				sqlc += fmt.Sprintf("%s,\n", c.Column)
			}
		}
		sqlc += fmt.Sprintf(")\n")
	}
	return sqld, sqlc
}

//GenLink generate table creation
func (db *Database) GenLink(dst Dbase, src Dbase, s, t string, cols []Columns, pkey []PKey) (sqld, sqlc string) {
	tmp := ""
	if t == strings.ToUpper(t) {
		tmp = "TEMP"
	} else {
		tmp = "temp"
	}
	clen := len(cols)
	if dst.Driver == "postgres" {
		sqld += fmt.Sprintf("DROP FOREIGN TABLE IF EXISTS \"%s\".\"%s%s\" CASCADE;\n", s, t, tmp)
		sqlc += fmt.Sprintf("CREATE FOREIGN TABLE IF NOT EXISTS \"%s\".\"%s%s\" (\n", s, t, tmp)
		for k, c := range cols {
			if k == clen-1 {
				sqlc += fmt.Sprintf("%s\n", c.Column)
			} else {
				sqlc += fmt.Sprintf("%s,\n", c.Column)
			}
		}
		sqlc += fmt.Sprintf(")\n")
		sqlc += fmt.Sprintf("SERVER %s \nOPTIONS (", src.Name)
		sqlc += fmt.Sprintf("table_name '%s.%s', ", s, t)
		sqlc += fmt.Sprintf("row_estimate_method 'showplan_all', ")
		sqlc += fmt.Sprintf("match_column_names '0');")
	} else if dst.Driver == "mssql" {
		sqld += fmt.Sprintf("DROP VIEW \"%s\".\"%s%s\";\n", s, t, tmp)
		sqlc += fmt.Sprintf("CREATE VIEW \"%s\".\"%s%s\" AS\n", s, t, tmp)
		for k, c := range cols {
			collation := ""
			if c.DataType == "CHAR" ||
				c.DataType == "VARCHAR" ||
				c.DataType == "NCHAR" ||
				c.DataType == "NVARCHAR" {
				collation = "COLLATE database_default "
			}
			if k == clen-1 {
				sqlc += fmt.Sprintf("\"%s\" %s\"%s\"\n", c.ColumnName, collation, c.ColumnName)
			} else {
				sqlc += fmt.Sprintf("\"%s\" %s\"%s\",\n", c.ColumnName, collation, c.ColumnName)
			}
		}
		sqlc += fmt.Sprintf("FROM \"%s\".\"%s\".\"%s\".\"%s\";\n", src.Host, src.Database, s, t)
	}
	return sqld, sqlc
}

// func remove(slice[]string,s int)[]string{
// 	return append(slice[:s],slice[s+1:]...)
// }

// function to reverse the given integer array
func reverse(numbers []int) []int {

	var length = len(numbers) // getting length of an array

	for i := 0; i < length/2; i++ {
		temp := numbers[i]
		numbers[i] = numbers[length-i-1]
		numbers[length-i-1] = temp
	}

	return numbers
}

func removeColumn(slice []Columns, s int) []Columns {
	return append(slice[:s], slice[s+1:]...)
}

func trimCols(cols []Columns, pkey []PKey) []Columns {
	var clist []int
	var ilist []int
	for k, c := range cols {
		clist = append(clist, k)
		for _, p := range pkey {
			if c.ColumnName == p.PKey {
				ilist = append(ilist, k)
			}
		}
	}
	// fmt.Println(clist)
	// fmt.Println(ilist)

	var collist []int
	for _, c := range clist {
		if func(e int, ee []int) bool {
			for _, i := range ee {
				if e == i {
					return false
				}
			}
			return true
		}(c, ilist) {
			collist = append(collist, c)
		}
	}
	// fmt.Println(collist)

	var columns []Columns
	for _, c := range collist {
		columns = append(columns, cols[c])
	}
	// fmt.Println(columns)
	return columns
}

//GenUpdate generate update procedure
func (db *Database) GenUpdate(dst Dbase, src Dbase, s, t string, cols []Columns, pkey []PKey) (sqld, sqlc string) {
	columns := trimCols(cols, pkey)

	sqld, sqlc = tableUpdProcStart(dst.Driver, s, t)
	sqlc += tableIndexSQL(dst.Driver, t, pkey)
	sqlc += tableDeleteSQL(dst.Driver, s, t, pkey, cols)
	if len(pkey) != len(cols) {
		sqlc += tableUpdateSQL(dst.Driver, s, t, pkey, columns)
	}
	sqlc += tableInsertSQL(dst.Driver, s, t, pkey, cols)
	sqlc += tableUpdProcEnd(dst.Driver, t)
	return sqld, sqlc
}

func tableUpdProcStart(destDriver, schema, tableName string) (sqld, sqlc string) {
	// upd := ""
	tmp := ""
	if tableName == strings.ToUpper(tableName) {
		// upd = "UPD"
		tmp = "TEMP"
	} else {
		// upd = "upd"
		tmp = "temp"
	}
	if destDriver == "postgres" {
		sqlc += fmt.Sprintf("CREATE OR REPLACE PROCEDURE \"%s\".\"upd_%s\"()\nLANGUAGE plpgsql\nAS $procedure$\nBEGIN\n", schema, tableName)
		sqlc += fmt.Sprintf("DROP TABLE IF EXISTS \"temp_%s\";\n", tableName)
		sqlc += fmt.Sprintf("CREATE TEMPORARY TABLE \"temp_%s\" AS SELECT * FROM \"%s\".\"%s%s\";\n", tableName, schema, tableName, tmp)
	} else if destDriver == "mssql" {
		sqld += fmt.Sprintf("DROP PROCEDURE \"%s\".\"upd_%s\";", schema, tableName)
		sqlc += fmt.Sprintf("CREATE PROCEDURE \"%s\".\"upd_%s\" AS\nBEGIN\n", schema, tableName)
		sqlc += fmt.Sprintf("IF OBJECT_ID('tempdb..#%s','U') IS NOT NULL DROP TABLE tempdb.#%s\n", tableName, tableName)
		sqlc += fmt.Sprintf("SELECT * INTO #%s FROM \"%s\".\"%s%s\"\n", tableName, schema, tableName, tmp)
	}
	return sqld, sqlc
}

func tableUpdProcEnd(destDriver, tableName string) (sqlc string) {
	if destDriver == "postgres" {
		sqlc += fmt.Sprintf("DROP TABLE IF EXISTS \"temp_%s\";\n", tableName)
		sqlc += fmt.Sprintf("END\n$procedure$;\n")
	} else if destDriver == "mssql" {
		sqlc += fmt.Sprintf("IF OBJECT_ID('tempdb..#%s','U') IS NOT NULL DROP TABLE tempdb.#%s\n", tableName, tableName)
		sqlc += fmt.Sprintf("END;\n")
	}
	return sqlc
}

func tableIndexSQL(destDriver, tableName string, pkey []PKey) (sqlc string) {
	if destDriver == "postgres" {
		sqlc += fmt.Sprintf("CREATE INDEX \"tp_%s\" ON \"temp_%s\" (", tableName, tableName)
	} else if destDriver == "mssql" {
		sqlc += fmt.Sprintf("CREATE INDEX \"tp_%s\" ON #%s (", tableName, tableName)
	}
	plen := len(pkey)
	for k, p := range pkey {
		sqlc += fmt.Sprintf("\"%s\"", p.PKey)
		if k == plen-1 {
			sqlc += ""
		} else {
			sqlc += ","
		}
	}
	if destDriver == "postgres" {
		sqlc += ");\n"
	} else if destDriver == "mssql" {
		sqlc += ")\n"
	}
	return sqlc
}

func tableDeleteSQL(destDriver, schema, tableName string, pkey []PKey, allColumns []Columns) (sqlc string) {
	if destDriver == "postgres" {
		sqlc += "DELETE\n"
	} else if destDriver == "mssql" {
		sqlc += fmt.Sprintf("DELETE \"%s\".\"%s\"\n", schema, tableName)
	}
	sqlc += fmt.Sprintf("FROM \"%s\".\"%s\"\n", schema, tableName)
	if destDriver == "postgres" {
		sqlc += fmt.Sprintf("USING \"%s\".\"%s\" AS d\n", schema, tableName)
		sqlc += fmt.Sprintf("LEFT OUTER JOIN \"temp_%s\" \"%stemp\" ON", tableName, tableName)
	} else if destDriver == "mssql" {
		sqlc += fmt.Sprintf("LEFT JOIN #%s \"%stemp\" ON", tableName, tableName)
	}
	plen := len(pkey)
	for k, p := range pkey {
		if destDriver == "postgres" {
			sqlc += fmt.Sprintf("\nd.\"%s\" = \"%stemp\".\"%s\"", p.PKey, tableName, p.PKey)
		} else if destDriver == "mssql" {
			sqlc += fmt.Sprintf("\n\"%s\".\"%s\" = \"%stemp\".\"%s\"", tableName, p.PKey, tableName, p.PKey)
		}
		if k == plen-1 {
			sqlc += "\n"
		} else {
			sqlc += " AND "
		}
	}
	if destDriver == "postgres" {
		sqlc += "WHERE"
	}
	if destDriver == "postgres" {
		for k, p := range pkey {
			sqlc += fmt.Sprintf("\n\"%s\".\"%s\" = d.\"%s\" ", tableName, p.PKey, p.PKey)
			if k == plen-1 {
				sqlc += "\n"
			} else {
				sqlc += " AND "
			}
		}
	}
	if destDriver == "postgres" {
		sqlc += fmt.Sprintf("AND \"%stemp\".\"%s\" IS NULL;\n", tableName, allColumns[0].ColumnName)

	} else if destDriver == "mssql" {
		sqlc += fmt.Sprintf("WHERE \"%stemp\".\"%s\" IS NULL\n", tableName, allColumns[0].ColumnName)
	}
	return sqlc
}

func tableUpdateSQL(destDriver, schema, tableName string, pkey []PKey, columns []Columns) (sqlc string) {
	sqlc += fmt.Sprintf("UPDATE \"%s\".\"%s\"\nSET", schema, tableName)
	plen := len(pkey)
	clen := len(columns)
	for k, c := range columns {
		sqlc += fmt.Sprintf("\n\"%s\" = \"%stemp\".\"%s\"", c.ColumnName, tableName, c.ColumnName)
		if k == clen-1 {
			sqlc += ""
		} else {
			sqlc += ","
		}
	}
	if destDriver == "postgres" {
		sqlc += fmt.Sprintf("\nFROM \"temp_%s\" \"%stemp\"", tableName, tableName)
	} else if destDriver == "mssql" {
		sqlc += fmt.Sprintf("\nFROM #%s \"%stemp\"", tableName, tableName)
	}
	if destDriver == "postgres" {
		sqlc += "\nWHERE"
	} else if destDriver == "mssql" {
		sqlc += fmt.Sprintf("\nJOIN \"%s\".\"%s\" ON", schema, tableName)
	}
	for k, p := range pkey {
		sqlc += fmt.Sprintf("\n\"%s\".\"%s\" = \"%stemp\".\"%s\"", tableName, p.PKey, tableName, p.PKey)
		if k == plen-1 {
			sqlc += "\n"
		} else {
			sqlc += " AND "
		}
	}
	if destDriver == "postgres" {
		sqlc += "AND ("
	} else if destDriver == "mssql" {
		sqlc += "WHERE ("
	}
	for k, c := range columns {
		sqlc += fmt.Sprintf("\n\"%s\".\"%s\" <> \"%stemp\".\"%s\"", tableName, c.ColumnName, tableName, c.ColumnName)
		if k == clen-1 {
			sqlc += "\n"
		} else {
			sqlc += " OR "
		}
	}
	if destDriver == "postgres" {
		sqlc += ");\n"
	} else if destDriver == "mssql" {
		sqlc += ")\n"
	}
	return sqlc
}

func tableInsertSQL(destDriver, schema, tableName string, pkey []PKey, allColumns []Columns) (sqlc string) {
	plen := len(pkey)
	clen := len(allColumns)
	sqlc += fmt.Sprintf("INSERT INTO \"%s\".\"%s\"\n", schema, tableName)
	sqlc += "SELECT"
	for k, c := range allColumns {
		sqlc += fmt.Sprintf("\n\"%stemp\".\"%s\" \"%s\"", tableName, c.ColumnName, c.ColumnName)
		if k == clen-1 {
			sqlc += "\n"
		} else {
			sqlc += ","
		}
	}
	sqlc += fmt.Sprintf("FROM \"%s\".\"%s\"\n", schema, tableName)
	if destDriver == "postgres" {
		sqlc += fmt.Sprintf("RIGHT JOIN \"temp_%s\" \"%stemp\" ON", tableName, tableName)
	} else if destDriver == "mssql" {
		sqlc += fmt.Sprintf("RIGHT JOIN #%s \"%stemp\" ON", tableName, tableName)
	}
	for k, p := range pkey {
		sqlc += fmt.Sprintf("\n\"%s\".\"%s\" = \"%stemp\".\"%s\"", tableName, p.PKey, tableName, p.PKey)
		if k == plen-1 {
			sqlc += "\n"
		} else {
			sqlc += " AND "
		}
	}
	if destDriver == "postgres" {
		sqlc += fmt.Sprintf("WHERE \"%s\".\"%s\" IS NULL;\n", tableName, allColumns[0].ColumnName)
	} else if destDriver == "mssql" {
		sqlc += fmt.Sprintf("WHERE \"%s\".\"%s\" IS NULL\n", tableName, allColumns[0].ColumnName)
	}
	return sqlc
}
