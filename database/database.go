package database

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

func checkErr(err error) {
	if err != nil {
		fmt.Print("Error:", err)
		panic(err)
	}
}

// Database struct contains sql pointer
type Database struct {
	*sqlx.DB
}

// Dbases array of Dbase
type Dbases struct {
	DB []Dbase `json:"dbases"`
}

// Dbase for loading from json
type Dbase struct {
	Name     string   `json:"name"`
	Driver   string   `json:"driver"`
	Host     string   `json:"host"`
	Port     string   `json:"port"`
	Database string   `json:"database"`
	Schema   []string `json:"schema"`
	Username string   `json:"username"`
	Password string   `json:"password"`
	PoolSize string   `json:"poolsize"`
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

//ExecProcedure executes stored procedure
func (db *Database) ExecProcedure(q string) {
	fmt.Println(q)
	_, err := db.Exec(q)
	if err != nil {
		panic(err)
	}
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

func tableDeleteSQL(destDriver, schema, tableName string, pkey []PKey, allColumns []Column) (sqlc string) {
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

func tableUpdateSQL(destDriver, schema, tableName string, pkey []PKey, columns []Column) (sqlc string) {
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

func tableInsertSQL(destDriver, schema, tableName string, pkey []PKey, allColumns []Column) (sqlc string) {
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

// Utilities

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

func removeColumn(slice []Column, s int) []Column {
	return append(slice[:s], slice[s+1:]...)
}

func trimCols(cols []Column, pkey []PKey) []Column {
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

	var columns []Column
	for _, c := range collist {
		columns = append(columns, cols[c])
	}
	// fmt.Println(columns)
	return columns
}
