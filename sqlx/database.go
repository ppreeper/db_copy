package dbc

import (
	"fmt"

	_ "github.com/denisenkom/go-mssqldb" //mssql driver
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq" //postgresql driver
	// _ "github.com/denisenkom/go-mssqldb"
	// _ "github.com/fajran/go-monetdb" //Monet
	// _ "github.com/jmoiron/sqlx"
	// _ "github.com/lib/pq" //postgresql
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
		fmt.Errorf("Open sql (%v): %v", dburi, err)
		panic(err)
	}
	if err = db.Ping(); err != nil {
		fmt.Errorf("Ping sql: %v", err)
		panic(err)
	}
	return &db, err
}

//TableNames list of tables
type TableNames struct {
	TableName string `db:"TABLE_NAME"`
}

// GetTables returns table list
func (db *Database) GetTables(d Dbase) ([]TableNames, error) {
	q := ""
	if d.Driver == "postgres" {
		q += "select tablename \"TABLE_NAME\"\n"
		q += "from " + d.Database + ".pg_catalog.pg_tables\n"
		q += "where schemaname = '" + d.Schema + "'\n"
		q += "order by tablename"
	} else if d.Driver == "mssql" {
		q += "SELECT TABLE_NAME \n"
		q += "FROM \"" + d.Database + "\".\"INFORMATION_SCHEMA\".\"TABLES\"\n"
		q += "WHERE TABLE_CATALOG = '" + d.Database + "' AND TABLE_SCHEMA = '" + d.Schema + "'\n"
		q += "AND TABLE_TYPE = 'BASE TABLE'\n"
		q += "ORDER BY TABLE_NAME"
	}
	// fmt.Println(q)
	tablenames := []TableNames{}
	// tablename := TableNames{}
	if err := db.Select(&tablenames, q); err != nil {
		return nil, fmt.Errorf("Select: %v", err)
	}
	return tablenames, nil
}

//Columns struct
type Columns struct {
	Column string `db:"CL"`
}

//ExecProcedure executes stored procedure
func (db *Database) ExecProcedure(q string) {
	fmt.Println(q)
	_, err := db.Exec(q)
	if err != nil {
		panic(err)
	}
}

//GetColumnDetail func
func (db *Database) GetColumnDetail(d Dbase, s Dbase, t string) ([]Columns, error) {
	q := ""
	if d.Driver == "postgres" {
		if s.Driver == "mssql" {
			q += "-- mssql to pgsql" + "\n"
			q += "SELECT" + "\n"
			q += "'\"'+C.COLUMN_NAME + '\" ' +" + "\n"
			q += "CASE UPPER(DATA_TYPE)" + "\n"
			q += "WHEN 'CHAR' THEN 'CHARACTER' + '(' + CONVERT(VARCHAR,C.CHARACTER_MAXIMUM_LENGTH)+')'" + "\n"
			q += "WHEN 'NCHAR' THEN 'CHARACTER' + '(' + CONVERT(VARCHAR,C.CHARACTER_MAXIMUM_LENGTH)+')'" + "\n"
			q += "WHEN 'VARCHAR' THEN 'CHARACTER VARYING' + '(' + CONVERT(VARCHAR,C.CHARACTER_MAXIMUM_LENGTH)+')'" + "\n"
			q += "WHEN 'NVARCHAR' THEN 'CHARACTER VARYING' + '(' + CONVERT(VARCHAR,C.CHARACTER_MAXIMUM_LENGTH)+')'" + "\n"
			q += "WHEN 'TINYINT' THEN 'INT'" + "\n"
			q += "WHEN 'SMALLINT' THEN 'SMALLINT'" + "\n"
			q += "WHEN 'INT' THEN 'INT'" + "\n"
			q += "WHEN 'DECIMAL' THEN 'DECIMAL' + '(' + CONVERT(VARCHAR,C.NUMERIC_PRECISION) + ',' + CONVERT(VARCHAR,C.NUMERIC_SCALE) + ')'" + "\n"
			q += "WHEN 'FLOAT' THEN 'FLOAT' + CASE WHEN C.NUMERIC_PRECISION < 53 THEN '(' + CONVERT(VARCHAR,C.NUMERIC_PRECISION) + ')' ELSE '' END" + "\n"
			q += "WHEN 'VARBINARY' THEN 'BYTEA'" + "\n"
			q += "WHEN 'DATETIME' THEN 'TIMESTAMP'" + "\n"
			q += "ELSE DATA_TYPE" + "\n"
			q += "END  + ' ' +" + "\n"
			q += "CASE WHEN IS_NULLABLE = 'NO' THEN 'NOT NULL' ELSE '' END  + ' ' +" + "\n"
			q += "CASE WHEN C.COLUMN_DEFAULT IS NULL THEN ''" + "\n"
			q += "ELSE ' DEFAULT ' +" + "\n"
			q += "REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(" + "\n"
			q += "REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(" + "\n"
			q += "REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(COLUMN_DEFAULT,'create default ',''),'_default as ',''),'numc',''),'num',''),'empstr',''),'str','')" + "\n"
			q += ",'10',''),'20',''),'30',''),'40',''),'50',''),'60',''),'70',''),'80',''),'90','')" + "\n"
			q += ",'1',''),'2',''),'3',''),'4',''),'5',''),'6',''),'7',''),'8',''),'9','')" + "\n"
			q += "END CL" + "\n"
			q += "FROM " + s.Database + ".\"INFORMATION_SCHEMA\".\"COLUMNS\" C" + "\n"
			q += "WHERE C.TABLE_CATALOG = '" + s.Database + "' AND C.TABLE_SCHEMA = '" + s.Schema + "'" + "\n"
			q += "AND C.TABLE_NAME IN ('" + t + "') " + "\n"
			q += "ORDER BY C.TABLE_NAME, C.ORDINAL_POSITION" + "\n"
		} else if s.Driver == "postgres" {
			q += "-- pgsql to pgsql" + "\n"
			q += "select" + "\n"
			q += "'\"' || c.column_name || '\" ' ||" + "\n"
			q += "case(c.data_type) " + "\n"
			q += "when 'character' then 'character' || '(' || c.character_maximum_length || ')'" + "\n"
			q += "when 'character varying' then 'character varying' || '(' || c.character_maximum_length || ')'" + "\n"
			q += "when 'decimal' then 'decimal' || '(' || c.numeric_precision||','||c.numeric_scale || ')'" + "\n"
			q += "when 'numeric' then 'numeric' || '(' || c.numeric_precision||','||c.numeric_scale || ')'" + "\n"
			q += "when 'real' then 'real' || '(' || c.numeric_precision||','||c.numeric_scale || ')'" + "\n"
			q += "when 'double precision' then 'double precision' || '(' || c.numeric_precision||','||c.numeric_precision_radix || ')'" + "\n"
			q += "else c.data_type" + "\n"
			q += "end || ' ' ||" + "\n"
			q += "CASE WHEN c.is_nullable = 'NO' THEN 'NOT NULL' ELSE '' END  || ' ' ||" + "\n"
			q += "CASE WHEN C.COLUMN_DEFAULT IS NULL THEN '' else 'DEFAULT ' || c.column_default END" + "\n"
			q += "\"CL\"" + "\n"
			q += "FROM information_schema.columns c" + "\n"
			q += "WHERE table_catalog = '" + s.Database + "'" + "\n"
			q += "AND table_schema = '" + s.Schema + "'" + "\n"
			q += "and table_name = '" + t + "'" + "\n"
			q += "ORDER BY c.table_catalog,table_schema,table_name,c.ordinal_position" + "\n"
		}
	} else if d.Driver == "mssql" {
		if s.Driver == "mssql" {
			q += "-- mssql to mssql" + "\n"
			q += "SELECT" + "\n"
			q += "'\"'+C.COLUMN_NAME + '\" ' +" + "\n"
			q += "CASE UPPER(DATA_TYPE)" + "\n"
			q += "WHEN 'CHAR' THEN 'CHAR' + '(' + CONVERT(VARCHAR,C.CHARACTER_MAXIMUM_LENGTH)+')'" + "\n"
			q += "WHEN 'NCHAR' THEN 'NCHAR' + '(' + CONVERT(VARCHAR,C.CHARACTER_MAXIMUM_LENGTH)+')'" + "\n"
			q += "WHEN 'VARCHAR' THEN 'VARCHAR' + '(' + CONVERT(VARCHAR,C.CHARACTER_MAXIMUM_LENGTH)+')'" + "\n"
			q += "WHEN 'NVARCHAR' THEN 'NVARCHAR' + '(' + CONVERT(VARCHAR,C.CHARACTER_MAXIMUM_LENGTH)+')'" + "\n"
			q += "WHEN 'TINYINT' THEN 'TINYINT'" + "\n"
			q += "WHEN 'SMALLINT' THEN 'SMALLINT'" + "\n"
			q += "WHEN 'INT' THEN 'INT'" + "\n"
			q += "WHEN 'DECIMAL' THEN 'DECIMAL' + '(' + CONVERT(VARCHAR,C.NUMERIC_PRECISION) + ',' + CONVERT(VARCHAR,C.NUMERIC_SCALE) + ')'" + "\n"
			q += "WHEN 'FLOAT' THEN 'FLOAT' + CASE WHEN C.NUMERIC_PRECISION < 53 THEN '(' + CONVERT(VARCHAR,C.NUMERIC_PRECISION) + ')' ELSE '' END" + "\n"
			q += "WHEN 'VARBINARY' THEN 'VARBINARY'" + "\n"
			q += "WHEN 'DATETIME' THEN 'DATETIME'" + "\n"
			q += "ELSE DATA_TYPE" + "\n"
			q += "END  + ' ' +" + "\n"
			q += "CASE WHEN IS_NULLABLE = 'NO' THEN 'NOT NULL' ELSE '' END  + ' ' +" + "\n"
			q += "CASE WHEN C.COLUMN_DEFAULT IS NULL THEN ''" + "\n"
			q += "ELSE ' DEFAULT ' +" + "\n"
			q += "REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(" + "\n"
			q += "REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(" + "\n"
			q += "REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(COLUMN_DEFAULT,'create default ',''),'_default as ',''),'numc',''),'num',''),'empstr',''),'str','')" + "\n"
			q += ",'10',''),'20',''),'30',''),'40',''),'50',''),'60',''),'70',''),'80',''),'90','')" + "\n"
			q += ",'1',''),'2',''),'3',''),'4',''),'5',''),'6',''),'7',''),'8',''),'9','')" + "\n"
			q += "END CL" + "\n"
			q += "FROM " + s.Database + ".\"INFORMATION_SCHEMA\".\"COLUMNS\" C" + "\n"
			q += "WHERE C.TABLE_CATALOG = '" + s.Database + "' AND C.TABLE_SCHEMA = '" + s.Schema + "'" + "\n"
			q += "AND C.TABLE_NAME IN ('" + t + "') " + "\n"
			q += "ORDER BY C.TABLE_NAME, C.ORDINAL_POSITION" + "\n"
		} else if s.Driver == "postgres" {
			q += "-- pgsql to mssql" + "\n"
			q += "SELECT " + "\n"
			q += "'\"'||c.column_name || '\" ' ||  " + "\n"
			q += "CASE UPPER(c.data_type)" + "\n"
			q += "WHEN 'CHARACTER' THEN 'CHAR' || '(' || C.CHARACTER_MAXIMUM_LENGTH || ')'" + "\n"
			q += "WHEN 'CHARACTER VARYING' THEN 'VARCHAR' || '(' || C.CHARACTER_MAXIMUM_LENGTH || ')'" + "\n"
			q += "WHEN 'SMALLINT' THEN 'SMALLINT'" + "\n"
			q += "WHEN 'INT' THEN 'INT'" + "\n"
			q += "WHEN 'DECIMAL' THEN 'DECIMAL' || '(' || C.NUMERIC_PRECISION || ',' || C.NUMERIC_SCALE || ')'" + "\n"
			q += "WHEN 'NUMERIC' THEN 'NUMERIC' || '(' || C.NUMERIC_PRECISION || ',' || C.NUMERIC_SCALE || ')'" + "\n"
			q += "WHEN 'DOUBLE PRECISION' THEN 'FLOAT' || '(' || C.NUMERIC_PRECISION || ',' || C.NUMERIC_PRECISION_RADIX || ')'" + "\n"
			q += "WHEN 'BYTEA' THEN 'VARBINARY'" + "\n"
			q += "WHEN 'TIMESTAMP WITHOUT TIME ZONE' THEN 'DATETIME'" + "\n"
			q += "ELSE DATA_TYPE" + "\n"
			q += "END || ' ' ||" + "\n"
			q += "CASE WHEN c.is_nullable = 'NO' THEN 'NOT NULL' ELSE '' END  || ' ' ||" + "\n"
			q += "CASE WHEN C.COLUMN_DEFAULT IS NULL THEN '' else 'DEFAULT ' || " + "\n"
			q += "replace(c.column_default,'::character varying','')" + "\n"
			q += "END" + "\n"
			q += "\"CL\"" + "\n"
			q += "FROM information_schema.columns c" + "\n"
			q += "WHERE table_catalog = '" + s.Database + "'" + "\n"
			q += "AND table_schema = '" + s.Schema + "'" + "\n"
			q += "AND table_name = '" + t + "'" + "\n"
			q += "ORDER BY c.table_catalog,table_schema,table_name,c.ordinal_position" + "\n"
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
func (db *Database) GetPKey(d Dbase, s Dbase, t string) ([]PKey, error) {
	q := ""
	if s.Driver == "postgres" {
		q += "select column_name \"CL\"" + "\n"
		q += "from sapdb.information_schema.constraint_column_usage" + "\n"
		q += "where table_catalog = '" + s.Database + "'" + "\n"
		q += "and table_schema = '" + s.Schema + "'" + "\n"
		q += "and table_name = '" + t + "'" + "\n"
		q += "and constraint_name in (" + "\n"
		q += "select constraint_name" + "\n"
		q += "from sapdb.information_schema.constraint_table_usage" + "\n"
		q += "where table_catalog = '" + s.Database + "'" + "\n"
		q += "and table_schema = '" + s.Schema + "'" + "\n"
		q += "and table_name = '" + t + "'" + "\n"
		q += ")" + "\n"
	} else if s.Driver == "mssql" {
		q += "SELECT COLUMN_NAME CL" + "\n"
		q += "FROM \"" + s.Database + "\".\"INFORMATION_SCHEMA\".\"CONSTRAINT_COLUMN_USAGE\" C" + "\n"
		q += "WHERE C.TABLE_CATALOG = '" + s.Database + "'" + "\n"
		q += "AND C.TABLE_SCHEMA = '" + s.Schema + "'" + "\n"
		q += "AND C.TABLE_NAME IN ('" + t + "')" + "\n"
		q += "AND C.CONSTRAINT_NAME IN (" + "\n"
		q += "SELECT CONSTRAINT_NAME" + "\n"
		q += "FROM \"" + s.Database + "\".\"INFORMATION_SCHEMA\".\"TABLE_CONSTRAINTS\" C" + "\n"
		q += "WHERE C.TABLE_CATALOG = '" + s.Database + "'" + "\n"
		q += " AND C.TABLE_SCHEMA = '" + s.Schema + "'" + "\n"
		q += "AND CONSTRAINT_TYPE = 'PRIMARY KEY'" + "\n"
		q += "AND C.TABLE_NAME IN ('" + t + "')" + "\n"
		q += ")"
	}
	// fmt.Println(q)
	pkey := []PKey{}
	if err := db.Select(&pkey, q); err != nil {
		return nil, fmt.Errorf("Select: %v", err)
	}
	return pkey, nil
}

//GenTable generate table craeation
func (db *Database) GenTable(d Dbase, t string, cols []Columns, pkey []PKey) string {
	q := ""
	if d.Driver == "postgres" {
		q += "DROP TABLE IF EXISTS \"" + d.Schema + "\".\"" + t + "\"" + " CASCADE;\n"
		q += "CREATE TABLE IF NOT EXISTS \"" + d.Schema + "\".\"" + t + "\" (" + "\n"
		clen := len(cols)
		plen := len(pkey)
		for k, c := range cols {
			if k == clen-1 {
				if plen > 0 {
					q += c.Column + ",\n"
					q += "PRIMARY KEY ("
					for v, p := range pkey {
						if v == plen-1 {
							q += "\"" + p.PKey + "\""
						} else {
							q += "\"" + p.PKey + "\","
						}
					}
					q += ")" + "\n"
				} else {
					q += c.Column + "\n"
				}
			} else {
				q += c.Column + ",\n"
			}
		}
		q += ")" + ";\n"
	} else if d.Driver == "mssql" {
		q += "CREATE TABLE " + d.Schema + "." + t + "(" + "\n"
		q += ")" + "\n"
	}
	return q
}

//GenLink generate table craeation
func (db *Database) GenLink(d Dbase, s Dbase, t string, cols []Columns, pkey []PKey) string {
	q := ""
	if d.Driver == "postgres" {
		q += "DROP FOREIGN TABLE IF EXISTS \"" + d.Schema + "\".\"" + t + "TEMP\" CASCADE;\n"
		q += "CREATE FOREIGN TABLE IF NOT EXISTS \"" + d.Schema + "\".\"" + t + "TEMP\" (" + "\n"
		clen := len(cols)
		for k, c := range cols {
			if k == clen-1 {
				q += c.Column + "\n"
			} else {
				q += c.Column + ",\n"
			}
		}
		q += ")"
		q += " SERVER " + s.Name + " OPTIONS (table_name '" + s.Schema + "." + t + "'"
		q += ", row_estimate_method 'showplan_all'"
		q += ", match_column_names '0'"
		q += ")" + ";\n"
	} else if d.Driver == "mssql" {
		q += "CREATE TABLE " + d.Schema + "." + t + "(" + "\n"
		q += ")" + "\n"
	}
	return q
}

//GenUpdate generate update procedure
func (db *Database) GenUpdate(d Dbase, s Dbase, t string, cols []Columns, pkey []PKey) string {
	q := ""
	if d.Driver == "postgres" {
		q += "DROP FUNCTION IF EXISTS \"" + d.Schema + "\".\"UPD_" + t + "\"();\n"
		q += "CREATE OR REPLACE FUNCTION \"" + d.Schema + "\".\"UPD_" + t + "\"()" + "\n"
		q += "RETURNS VOID AS $$\n"
		q += "BEGIN\n"
		q += "TRUNCATE TABLE \"" + d.Schema + "\".\"" + t + "\";\n"
		q += "INSERT INTO \"" + d.Schema + "\".\"" + t + "\" SELECT * FROM \"" + d.Schema + "\".\"" + t + "TEMP\"" + ";\n"
		q += "END;\n"
		q += "$$ LANGUAGE PLPGSQL;"
	}
	return q
}

//
// sapschemagen.go
//
//
// // TableCon structure
// type TableCon struct {
// 	ConstraintName string `db:"CONSTRAINT_NAME"`
// }
//
// // TablePKey get primary key
// func (db *Database) TablePKey(table string) ([]TableCon, error) {
// 	// fmt.Print("TablePKey: " + table + "\n")
// 	q := "SELECT CONSTRAINT_NAME "
// 	q += "FROM \"" + src.Dbname + "\".\"INFORMATION_SCHEMA\".\"TABLE_CONSTRAINTS\" C "
// 	q += "WHERE C.TABLE_CATALOG = '" + src.Dbname + "' AND C.TABLE_SCHEMA = '" + src.Schema + "' "
// 	q += "AND CONSTRAINT_TYPE = 'PRIMARY KEY' "
// 	q += "AND C.TABLE_NAME IN ('"
// 	q += table
// 	q += "')"
// 	// fmt.Println(q)
// 	tablecons := []TableCon{}
// 	tablecon := TableCon{}
// 	rows, err := db.Query(q)
// 	if err != nil {
// 		return nil, fmt.Errorf("Select: %v", err)
// 	}
// 	defer rows.Close()
// 	for rows.Next() {
// 		var constraintName string
// 		if err := rows.Scan(&constraintName); err != nil {
// 			log.Fatal(err)
// 		}
// 		tablecon.ConstraintName = constraintName
// 		tablecons = append(tablecons, tablecon)
// 	}
// 	// pretty.Println(tablecons)
// 	return tablecons, nil
// }
//
// // ColCon structure
// type ColCon struct {
// 	ColumnName string `db:"COLUMN_NAME"`
// }
//
// // ColumnPKey get primary key
// func (db *Database) ColumnPKey(table string, constraint string) ([]ColCon, error) {
// 	// fmt.Print("ColumnPKey: " + table + "\n")
// 	q := "SELECT COLUMN_NAME" + "\n"
// 	q += "FROM \"" + src.Dbname + "\".\"INFORMATION_SCHEMA\".\"CONSTRAINT_COLUMN_USAGE\" C" + "\n"
// 	q += "WHERE C.TABLE_CATALOG = '" + src.Dbname + "' AND C.TABLE_SCHEMA = '" + src.Schema + "'" + "\n"
// 	q += "AND C.TABLE_NAME IN ('" + table + "')" + "\n"
// 	q += "AND C.CONSTRAINT_NAME IN ('" + constraint + "')"
// 	// fmt.Print(q)
// 	colcons := []ColCon{}
// 	colcon := ColCon{}
// 	rows, err := db.Query(q)
// 	if err != nil {
// 		return nil, fmt.Errorf("Select: %v", err)
// 	}
// 	defer rows.Close()
// 	for rows.Next() {
// 		var columnName string
// 		if err := rows.Scan(&columnName); err != nil {
// 			log.Fatal(err)
// 		}
// 		colcon.ColumnName = columnName
// 		colcons = append(colcons, colcon)
// 	}
// 	// pretty.Println(colcons)
// 	return colcons, nil
// }
//
// func leftPad(s string, p string, c int) string {
// 	var t bytes.Buffer
// 	if c <= 0 {
// 		fmt.Println("Invalid Length of Padding")
// 		return ""
// 	}
// 	if len(p) < 1 {
// 		fmt.Println("Invalid Pad string")
// 		return ""
// 	}
// 	for i := 0; i < (c - len(s)); i++ {
// 		t.WriteString(p)
// 	}
// 	t.WriteString(s)
// 	return t.String()
// }
//
// func genTables(db *Database, table string) string {
// 	// fmt.Print("genTables: " + table + "\n")
// 	var tsql string
// 	var t []string
// 	if len(table) > 0 {
// 		table = strings.ToUpper(table)
// 		t = append(t, table)
// 	} else {
// 		tables, err := db.Tables()
// 		if err != nil {
// 			log.Fatalf("%v", err)
// 		}
// 		for i := 0; i < len(tables); i++ {
// 			t = append(t, tables[i].TableName)
// 		}
// 	}
// 	for i := 0; i < len(t); i++ {
// 		tsql = "\n-- " + t[i] + "\n"
// 		ct := "CREATE TABLE `" + dst.Schema + "`.`" + t[i] + "`"
// 		tsql += ct + " (\n"
// 		pkey, err := db.TablePKey(t[i])
// 		if err != nil {
// 			log.Fatalf("%v", err)
// 		}
// 		if len(pkey) == 0 {
// 			columns, err := db.Columns(t[i])
// 			if err != nil {
// 				log.Fatalf("%v", err)
// 			}
// 			lc := len(columns)
// 			var c string
// 			for j := 0; j < lc; j++ {
// 				if j == lc-1 {
// 					c = columns[j].Column
// 				} else {
// 					c = columns[j].Column + ","
// 				}
// 				c = strings.Replace(c, "VARBINARY", "BINARY", -1)
// 				// c = strings.Replace(c, "/", "_", -1)
// 				// c = strings.Replace(c, "\"", "", -1)
// 				tsql += c
// 			}
// 		} else {
// 			columns, err := db.Columns(t[i])
// 			if err != nil {
// 				log.Fatalf("%v", err)
// 			}
// 			lc := len(columns)
// 			var c string
// 			for j := 0; j < lc; j++ {
// 				c = columns[j].Column + ",\n"
// 				c = strings.Replace(c, "VARBINARY", "BINARY", -1)
// 				tsql += c
// 			}
// 			ckey, err := db.ColumnPKey(t[i], pkey[0].ConstraintName)
// 			if err != nil {
// 				log.Fatalf("%v", err)
// 			}
// 			key := "CONSTRAINT "
// 			key += "`" + t[i] + "_PKEY` PRIMARY KEY ("
// 			for k := 0; k < len(ckey); k++ {
// 				var ck string
// 				if dst.Dbtype == pg {
// 					ck = ckey[k].ColumnName
// 				} else {
// 					ck = "`" + ckey[k].ColumnName + "`"
// 				}
// 				if k == len(ckey)-1 {
// 					key += ck
// 				} else {
// 					key += ck + ","
// 				}
// 			}
// 			key += ")"
// 			tsql += key + "\n"
// 		}
// 		tsql += ");\n"
// 	}
//
// 	return tsql
// }

//
// sapschemagen-old
//

//
// // TableCon structure
// type TableCon struct {
// 	ConstraintName string `db:"CONSTRAINT_NAME"`
// }
//
// // TablePKey get primary key
// func (db *Database) TablePKey(table string) ([]TableCon, error) {
// 	q := `SELECT CONSTRAINT_NAME
// 				FROM "EP1"."INFORMATION_SCHEMA".TABLE_CONSTRAINTS C
// 				WHERE C.TABLE_CATALOG = 'EP1' AND C.TABLE_SCHEMA = 'ep1'
// 				AND CONSTRAINT_TYPE = 'PRIMARY KEY'
// 				AND C.TABLE_NAME IN ('`
// 	q += table
// 	q += `')`
// 	// fmt.Println(q)
// 	tablecons := []TableCon{}
// 	tablecon := TableCon{}
// 	rows, err := db.Query(q)
// 	if err != nil {
// 		return nil, fmt.Errorf("Select: %v", err)
// 	}
// 	defer rows.Close()
// 	for rows.Next() {
// 		var constraintName string
// 		if err := rows.Scan(&constraintName); err != nil {
// 			log.Fatal(err)
// 		}
// 		tablecon.ConstraintName = constraintName
// 		tablecons = append(tablecons, tablecon)
// 	}
// 	return tablecons, nil
// }
//
// // ColCon structure
// type ColCon struct {
// 	ColumnName string `db:"COLUMN_NAME"`
// }
//
// // ColumnPKey get primary key
// func (db *Database) ColumnPKey(table string, constraint string) ([]ColCon, error) {
// 	q := `SELECT COLUMN_NAME
// 				FROM "EP1"."INFORMATION_SCHEMA"."CONSTRAINT_COLUMN_USAGE" C
// 				WHERE C.TABLE_CATALOG = 'EP1' AND C.TABLE_SCHEMA = 'ep1'
// 				AND C.TABLE_NAME IN ('`
// 	q += table
// 	q += `')
// 				AND C.CONSTRAINT_NAME IN ('`
// 	q += constraint
// 	q += `')`
// 	colcons := []ColCon{}
// 	colcon := ColCon{}
// 	rows, err := db.Query(q)
// 	if err != nil {
// 		return nil, fmt.Errorf("Select: %v", err)
// 	}
// 	defer rows.Close()
// 	for rows.Next() {
// 		var columnName string
// 		if err := rows.Scan(&columnName); err != nil {
// 			log.Fatal(err)
// 		}
// 		colcon.ColumnName = columnName
// 		colcons = append(colcons, colcon)
// 	}
// 	return colcons, nil
// }
//
// func leftPad(s string, p string, c int) string {
// 	var t bytes.Buffer
// 	if c <= 0 {
// 		fmt.Println("Invalid Length of Padding")
// 		return ""
// 	}
// 	if len(p) < 1 {
// 		fmt.Println("Invalid Pad string")
// 		return ""
// 	}
// 	for i := 0; i < (c - len(s)); i++ {
// 		t.WriteString(p)
// 	}
// 	t.WriteString(s)
// 	return t.String()
// }
//
// func genTables(db *Database) {
// 	tables, err := db.Tables()
// 	if err != nil {
// 		log.Fatalf("%v", err)
// 	}
//
// 	for i := 0; i < len(tables); i++ {
// 		fmt.Printf("--------\n-- %s\n--------", tables[i].TableName)
// 		fmt.Printf("\nCREATE TABLE %s.\"%s\" (\n", schema, tables[i].TableName)
// 		pkey, err := db.TablePKey(tables[i].TableName)
// 		if err != nil {
// 			log.Fatalf("%v", err)
// 		}
// 		if len(pkey) == 0 {
// 			columns, err := db.Columns(tables[i].TableName)
// 			if err != nil {
// 				log.Fatalf("%v", err)
// 			}
// 			lc := len(columns)
// 			for j := 0; j < lc; j++ {
// 				if j == lc-1 {
// 					fmt.Printf("%s\n", columns[j].Column)
// 				} else {
// 					fmt.Printf("%s,\n", columns[j].Column)
// 				}
// 			}
// 		} else {
// 			columns, err := db.Columns(tables[i].TableName)
// 			if err != nil {
// 				log.Fatalf("%v", err)
// 			}
// 			lc := len(columns)
// 			for j := 0; j < lc; j++ {
// 				fmt.Printf("%s,\n", columns[j].Column)
// 			}
// 			ckey, err := db.ColumnPKey(tables[i].TableName, pkey[0].ConstraintName)
// 			if err != nil {
// 				log.Fatalf("%v", err)
// 			}
// 			key := `CONSTRAINT "` + tables[i].TableName + `_PKEY" PRIMARY KEY (`
// 			for k := 0; k < len(ckey); k++ {
// 				if k == len(ckey)-1 {
// 					key += `"` + ckey[k].ColumnName + `"`
// 				} else {
// 					key += `"` + ckey[k].ColumnName + `",`
// 				}
// 			}
// 			key += `)`
// 			fmt.Printf("%s\n", key)
// 		}
// 		fmt.Printf(");\n")
// 	}
// 	return
// }
//
// func genLinks(db *Database) {
// 	tables, err := db.Tables()
// 	if err != nil {
// 		log.Fatalf("%v", err)
// 	}
//
// 	for i := 0; i < len(tables); i++ {
// 		fmt.Printf("--------\n-- %s\n--------", tables[i].TableName)
// 		fmt.Printf("\n-- DROP VIEW %s.\"%sTEMP\";", schema, tables[i].TableName)
// 		fmt.Printf("\nCREATE VIEW %s.\"%sTEMP\" AS", schema, tables[i].TableName)
// 		fmt.Printf("\nSELECT\n")
// 		columns, err := db.ColumnsLink(tables[i].TableName)
// 		if err != nil {
// 			log.Fatalf("%v", err)
// 		}
// 		lc := len(columns)
// 		for j := 0; j < lc; j++ {
// 			if j == lc-1 {
// 				fmt.Printf("%s\n", columns[j].Column)
// 			} else {
// 				fmt.Printf("%s,\n", columns[j].Column)
// 			}
// 		}
// 		key := `FROM "` + host
// 		key += `"."` + dbname
// 		key += `"."` + schema
// 		key += `"."` + tables[i].TableName
// 		key += `" "` + tables[i].TableName
// 		key += `TEMP"`
// 		fmt.Printf("%s;\n", key)
// 	}
// 	return
// }
//
// // main app
// func main() {
// 	//open database
// 	var dburi string
// 	dburi = "server=" + host
// 	dburi += ";user id=" + username
// 	dburi += ";password=" + password
// 	dburi += ";database=" + dbname
// 	dburi += ";encrypt=disable"
// 	dburi += ";connection timeout=7200"
// 	dburi += ";keepAlive=30"
//
// 	db, err := OpenDatabase("mssql", dburi)
// 	if err != nil {
// 		log.Fatalf("OpenDatabase: %v", err)
// 	}
// 	defer db.Close()
//
// 	if tbl {
// 		genTables(db)
// 	}
//
// 	if lnk {
// 		genLinks(db)
// 	}
//
// }

//
// mssqlschemagen
//
// package main
//
// // Notice in the import list there's one package prefaced by a ".",
// // which allows referencing functions in that package without naming the library in
// // the call (if using . "fmt", I can call Println as Println, not fmt.Println)
// import (
// 	"bytes"
// 	"flag"
// 	"fmt"
// 	"log"
//
// 	_ "github.com/denisenkom/go-mssqldb"
// 	"github.com/jmoiron/sqlx"
// )
//
// const strVERSION string = "0.1 compiled on 2017-10-13"
//
// const host string = "sapprd.arthomson.local"
// const username string = "artg"
// const password string = "Gasket2008"
// const dbname string = "EP1"
// const schema string = "ep1"
//
// var tbl bool
// var lnk bool
//
// func init() {
// 	// 		flag.Usage = func() {
// 	// 				fmt.Fprintf(os.Stderr, `usage: %s [-a|-m] <address>
// 	// 			 %s <address> [new value]
// 	//
// 	// Options:
// 	// `, path.Base(os.Args[0]), path.Base(os.Args[0]))
// 	// 				flag.PrintDefaults()
// 	// 		}
// 	// Flags
// 	flag.BoolVar(&tbl, "t", false, "table gen")
// 	flag.BoolVar(&lnk, "l", false, "link table gen")
//
// 	flag.Parse()
// }
//
// // Database structure for access methods
// type Database struct {
// 	*sqlx.DB
// }
//
// // OpenDatabase attempts to open the database specified by DataSource
// // and return a handle to it
// func OpenDatabase(driver string, dburi string) (*Database, error) {
// 	db := Database{}
// 	var err error
//
// 	db.DB, err = sqlx.Open(driver, dburi)
// 	if err != nil {
// 		return nil, fmt.Errorf("Open sql (%v): %v", dburi, err)
// 	}
//
// 	if err = db.Ping(); err != nil {
// 		return nil, fmt.Errorf("Ping sql: %v", err)
// 	}
//
// 	return &db, nil
// }
//
// // TableName structure
// type TableName struct {
// 	TableName string `db:"TABLE_NAME"`
// }
//
// // Tables get table names
// func (db *Database) Tables() ([]TableName, error) {
// 	q := `SELECT TABLE_NAME
// 			FROM "EP1"."INFORMATION_SCHEMA"."TABLES"
// 			WHERE TABLE_CATALOG = 'EP1' AND TABLE_SCHEMA = 'ep1'
// 			AND TABLE_TYPE = 'BASE TABLE'
// 			AND TABLE_NAME NOT LIKE 'sap%'
// 			ORDER BY TABLE_NAME`
// 	tablenames := []TableName{}
// 	tablename := TableName{}
// 	rows, err := db.Query(q)
// 	if err != nil {
// 		return nil, fmt.Errorf("Select: %v", err)
// 	}
// 	defer rows.Close()
// 	for rows.Next() {
// 		var tableName string
// 		if err := rows.Scan(&tableName); err != nil {
// 			log.Fatal(err)
// 		}
// 		tablename.TableName = tableName
// 		tablenames = append(tablenames, tablename)
// 	}
// 	return tablenames, nil
// }
//
// // ColumnName structure
// type ColumnName struct {
// 	TableName string `db:"TABLE_NAME"`
// 	Column    string `db:"CL"`
// 	Ordinal   int    `db:ORDINAL_POSITION`
// }
//
// // Columns get table names
// func (db *Database) Columns(table string) ([]ColumnName, error) {
// 	q := `SELECT TABLE_NAME,
// 				'"'+COLUMN_NAME+'"' + ' ' +
// 				CASE UPPER(DATA_TYPE)
// 				WHEN 'CHAR' THEN 'CHAR' + '('+CONVERT(VARCHAR,C.CHARACTER_MAXIMUM_LENGTH)+')'
// 				WHEN 'NCHAR' THEN 'NCHAR' + '('+CONVERT(VARCHAR,C.CHARACTER_MAXIMUM_LENGTH)+')'
// 				WHEN 'VARCHAR' THEN 'VARCHAR' + '('+CONVERT(VARCHAR,C.CHARACTER_MAXIMUM_LENGTH)+')'
// 				WHEN 'NVARCHAR' THEN 'NVARCHAR' + '('+CONVERT(VARCHAR,C.CHARACTER_MAXIMUM_LENGTH)+')'
// 				WHEN 'TINYINT' THEN 'TINYINT'
// 				WHEN 'SMALLINT' THEN 'SMALLINT'
// 				WHEN 'INT' THEN 'INT'
// 				WHEN 'DECIMAL' THEN 'DECIMAL' + '('+CONVERT(VARCHAR,C.NUMERIC_PRECISION)+','+CONVERT(VARCHAR,C.NUMERIC_SCALE)+')'
// 				WHEN 'FLOAT' THEN 'FLOAT' + (CASE WHEN C.NUMERIC_PRECISION < 53 THEN '('+ CONVERT(VARCHAR,C.NUMERIC_PRECISION) +')' ELSE '' END)
// 				WHEN 'VARBINARY' THEN 'VARBINARY'
// 				WHEN 'DATETIME' THEN 'DATETIME'
// 				ELSE DATA_TYPE
// 				END  + ' ' +
// 				CASE WHEN IS_NULLABLE = 'NO' THEN 'NOT NULL' ELSE '' END  + ' ' +
// 				CASE WHEN C.COLUMN_DEFAULT IS NULL THEN ''
// 				ELSE ' DEFAULT ' +
// 				REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
// 				REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
// 				REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(COLUMN_DEFAULT,'create default ',''),'_default as ',''),'numc',''),'num',''),'empstr',''),'str','')
// 				,'10',''),'20',''),'30',''),'40',''),'50',''),'60',''),'70',''),'80',''),'90','')
// 				,'1',''),'2',''),'3',''),'4',''),'5',''),'6',''),'7',''),'8',''),'9','')
// 				END  CL
// 				,C.ORDINAL_POSITION
// 				FROM "EP1"."INFORMATION_SCHEMA"."COLUMNS" C
// 				WHERE C.TABLE_CATALOG = 'EP1' AND C.TABLE_SCHEMA = 'ep1'
// 				AND C.TABLE_NAME IN ('`
// 	q += table
// 	q += `') ORDER BY C.TABLE_NAME, C.ORDINAL_POSITION`
// 	// fmt.Println(q)
// 	columnnames := []ColumnName{}
// 	columnname := ColumnName{}
// 	rows, err := db.Query(q)
// 	if err != nil {
// 		return nil, fmt.Errorf("Select: %v", err)
// 	}
// 	defer rows.Close()
// 	for rows.Next() {
// 		var tableName string
// 		var column string
// 		var ordinal int
// 		if err := rows.Scan(&tableName, &column, &ordinal); err != nil {
// 			log.Fatal(err)
// 		}
// 		columnname.TableName = tableName
// 		columnname.Column = column
// 		columnname.Ordinal = ordinal
// 		columnnames = append(columnnames, columnname)
// 	}
// 	return columnnames, nil
// }
//
// // ColumnsLink get table names
// func (db *Database) ColumnsLink(table string) ([]ColumnName, error) {
// 	q := `SELECT TABLE_NAME,
// 				'"'+TABLE_NAME+'TEMP"' + '.' +
// 				'"'+COLUMN_NAME+'"' + ' ' +
// 				CASE UPPER(DATA_TYPE)
// 				WHEN 'CHAR' THEN 'COLLATE database_default'
// 				WHEN 'NCHAR' THEN 'COLLATE database_default'
// 				WHEN 'VARCHAR' THEN 'COLLATE database_default'
// 				WHEN 'NVARCHAR' THEN 'COLLATE database_default'
// 				WHEN 'TINYINT' THEN ''
// 				WHEN 'SMALLINT' THEN ''
// 				WHEN 'INT' THEN ''
// 				WHEN 'DECIMAL' THEN ''
// 				WHEN 'FLOAT' THEN ''
// 				WHEN 'VARBINARY' THEN ''
// 				WHEN 'DATETIME' THEN ''
// 				ELSE DATA_TYPE
// 				END  + ' ' +
// 				'"'+COLUMN_NAME+'"'
// 				,C.ORDINAL_POSITION
// 				FROM "EP1"."INFORMATION_SCHEMA"."COLUMNS" C
// 				WHERE C.TABLE_CATALOG = 'EP1' AND C.TABLE_SCHEMA = 'ep1'
// 				AND C.TABLE_NAME IN ('`
// 	q += table
// 	q += `') ORDER BY C.TABLE_NAME, C.ORDINAL_POSITION`
// 	// fmt.Println(q)
// 	columnnames := []ColumnName{}
// 	columnname := ColumnName{}
// 	rows, err := db.Query(q)
// 	if err != nil {
// 		return nil, fmt.Errorf("Select: %v", err)
// 	}
// 	defer rows.Close()
// 	for rows.Next() {
// 		var tableName string
// 		var column string
// 		var ordinal int
// 		if err := rows.Scan(&tableName, &column, &ordinal); err != nil {
// 			log.Fatal(err)
// 		}
// 		columnname.TableName = tableName
// 		columnname.Column = column
// 		columnname.Ordinal = ordinal
// 		columnnames = append(columnnames, columnname)
// 	}
// 	return columnnames, nil
// }
//
// // TableCon structure
// type TableCon struct {
// 	ConstraintName string `db:"CONSTRAINT_NAME"`
// }
//
// // TablePKey get primary key
// func (db *Database) TablePKey(table string) ([]TableCon, error) {
// 	q := `SELECT CONSTRAINT_NAME
// 				FROM "EP1"."INFORMATION_SCHEMA".TABLE_CONSTRAINTS C
// 				WHERE C.TABLE_CATALOG = 'EP1' AND C.TABLE_SCHEMA = 'ep1'
// 				AND CONSTRAINT_TYPE = 'PRIMARY KEY'
// 				AND C.TABLE_NAME IN ('`
// 	q += table
// 	q += `')`
// 	// fmt.Println(q)
// 	tablecons := []TableCon{}
// 	tablecon := TableCon{}
// 	rows, err := db.Query(q)
// 	if err != nil {
// 		return nil, fmt.Errorf("Select: %v", err)
// 	}
// 	defer rows.Close()
// 	for rows.Next() {
// 		var constraintName string
// 		if err := rows.Scan(&constraintName); err != nil {
// 			log.Fatal(err)
// 		}
// 		tablecon.ConstraintName = constraintName
// 		tablecons = append(tablecons, tablecon)
// 	}
// 	return tablecons, nil
// }
//
// // ColCon structure
// type ColCon struct {
// 	ColumnName string `db:"COLUMN_NAME"`
// }
//
// // ColumnPKey get primary key
// func (db *Database) ColumnPKey(table string, constraint string) ([]ColCon, error) {
// 	q := `SELECT COLUMN_NAME
// 				FROM "EP1"."INFORMATION_SCHEMA"."CONSTRAINT_COLUMN_USAGE" C
// 				WHERE C.TABLE_CATALOG = 'EP1' AND C.TABLE_SCHEMA = 'ep1'
// 				AND C.TABLE_NAME IN ('`
// 	q += table
// 	q += `')
// 				AND C.CONSTRAINT_NAME IN ('`
// 	q += constraint
// 	q += `')`
// 	colcons := []ColCon{}
// 	colcon := ColCon{}
// 	rows, err := db.Query(q)
// 	if err != nil {
// 		return nil, fmt.Errorf("Select: %v", err)
// 	}
// 	defer rows.Close()
// 	for rows.Next() {
// 		var columnName string
// 		if err := rows.Scan(&columnName); err != nil {
// 			log.Fatal(err)
// 		}
// 		colcon.ColumnName = columnName
// 		colcons = append(colcons, colcon)
// 	}
// 	return colcons, nil
// }
//
// func leftPad(s string, p string, c int) string {
// 	var t bytes.Buffer
// 	if c <= 0 {
// 		fmt.Println("Invalid Length of Padding")
// 		return ""
// 	}
// 	if len(p) < 1 {
// 		fmt.Println("Invalid Pad string")
// 		return ""
// 	}
// 	for i := 0; i < (c - len(s)); i++ {
// 		t.WriteString(p)
// 	}
// 	t.WriteString(s)
// 	return t.String()
// }
//
// func genTables(db *Database) {
// 	tables, err := db.Tables()
// 	if err != nil {
// 		log.Fatalf("%v", err)
// 	}
//
// 	for i := 0; i < len(tables); i++ {
// 		fmt.Printf("--------\n-- %s\n--------", tables[i].TableName)
// 		fmt.Printf("\nCREATE TABLE %s.\"%s\" (\n", schema, tables[i].TableName)
// 		pkey, err := db.TablePKey(tables[i].TableName)
// 		if err != nil {
// 			log.Fatalf("%v", err)
// 		}
// 		if len(pkey) == 0 {
// 			columns, err := db.Columns(tables[i].TableName)
// 			if err != nil {
// 				log.Fatalf("%v", err)
// 			}
// 			lc := len(columns)
// 			for j := 0; j < lc; j++ {
// 				if j == lc-1 {
// 					fmt.Printf("%s\n", columns[j].Column)
// 				} else {
// 					fmt.Printf("%s,\n", columns[j].Column)
// 				}
// 			}
// 		} else {
// 			columns, err := db.Columns(tables[i].TableName)
// 			if err != nil {
// 				log.Fatalf("%v", err)
// 			}
// 			lc := len(columns)
// 			for j := 0; j < lc; j++ {
// 				fmt.Printf("%s,\n", columns[j].Column)
// 			}
// 			ckey, err := db.ColumnPKey(tables[i].TableName, pkey[0].ConstraintName)
// 			if err != nil {
// 				log.Fatalf("%v", err)
// 			}
// 			key := `CONSTRAINT "` + tables[i].TableName + `_PKEY" PRIMARY KEY (`
// 			for k := 0; k < len(ckey); k++ {
// 				if k == len(ckey)-1 {
// 					key += `"` + ckey[k].ColumnName + `"`
// 				} else {
// 					key += `"` + ckey[k].ColumnName + `",`
// 				}
// 			}
// 			key += `)`
// 			fmt.Printf("%s\n", key)
// 		}
// 		fmt.Printf(");\n")
// 	}
// 	return
// }
//
// func genLinks(db *Database) {
// 	tables, err := db.Tables()
// 	if err != nil {
// 		log.Fatalf("%v", err)
// 	}
//
// 	for i := 0; i < len(tables); i++ {
// 		fmt.Printf("--------\n-- %s\n--------", tables[i].TableName)
// 		fmt.Printf("\n-- DROP VIEW %s.\"%sTEMP\";", schema, tables[i].TableName)
// 		fmt.Printf("\nCREATE VIEW %s.\"%sTEMP\" AS", schema, tables[i].TableName)
// 		fmt.Printf("\nSELECT\n")
// 		columns, err := db.ColumnsLink(tables[i].TableName)
// 		if err != nil {
// 			log.Fatalf("%v", err)
// 		}
// 		lc := len(columns)
// 		for j := 0; j < lc; j++ {
// 			if j == lc-1 {
// 				fmt.Printf("%s\n", columns[j].Column)
// 			} else {
// 				fmt.Printf("%s,\n", columns[j].Column)
// 			}
// 		}
// 		key := `FROM "` + host
// 		key += `"."` + dbname
// 		key += `"."` + schema
// 		key += `"."` + tables[i].TableName
// 		key += `" "` + tables[i].TableName
// 		key += `TEMP"`
// 		fmt.Printf("%s;\n", key)
// 	}
// 	return
// }
//
// // main app
// func main() {
// 	//open database
// 	var dburi string
// 	dburi = "server=" + host
// 	dburi += ";user id=" + username
// 	dburi += ";password=" + password
// 	dburi += ";database=" + dbname
// 	dburi += ";encrypt=disable"
// 	dburi += ";connection timeout=7200"
// 	dburi += ";keepAlive=30"
//
// 	db, err := OpenDatabase("mssql", dburi)
// 	if err != nil {
// 		log.Fatalf("OpenDatabase: %v", err)
// 	}
// 	defer db.Close()
//
// 	if tbl {
// 		genTables(db)
// 	}
//
// 	if lnk {
// 		genLinks(db)
// 	}
//
// }
