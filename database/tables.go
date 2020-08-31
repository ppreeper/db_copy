package database

import (
	"fmt"
	"os"
	"strings"
)

//////////
// Tables
//////////

//Table list of tables
type Table struct {
	Name string `db:"TABLE_NAME"`
}

// GetTableList returns table list
func (db *Database) GetTableList(d Dbase, schemaName string) ([]Table, error) {
	q := ""
	if d.Driver == "postgres" {
		q += "select TABLE_NAME \"TABLE_NAME\" "
		q += "from INFORMATION_SCHEMA.TABLES "
		q += "where TABLE_SCHEMA = '" + schemaName + "' "
		q += "and TABLE_TYPE = 'BASE TABLE' "
		q += "order by TABLE_NAME"
	} else if d.Driver == "mssql" {
		q += "select TABLE_NAME \"TABLE_NAME\" "
		q += "from INFORMATION_SCHEMA.TABLES "
		q += "where TABLE_SCHEMA = '" + schemaName + "' "
		q += "and TABLE_TYPE = 'BASE TABLE' "
		q += "order by TABLE_NAME"
	}
	// fmt.Println(q)
	tt := []Table{}
	if err := db.Select(&tt, q); err != nil {
		return nil, fmt.Errorf("Select: %v", err)
	}
	return tt, nil
}

// GetTableSchema gets table definition
func (db *Database) GetTableSchema(d Dbase, schema, table string, dbg bool) {
	fmt.Printf("\n-- TABLE: %s.%s", schema, table)
	scols, err := db.GetColumnDetail(d, Dbase{}, schema, table)
	checkErr(err)
	// fmt.Println(scols)
	pcols, err := db.GetPKey(d, Dbase{}, schema, table)
	checkErr(err)
	// fmt.Println(pcols)

	sqld, sqlc := db.GenTable(d, schema, table, scols, pcols)

	if dbg {
		fmt.Printf("\n%v\n%v\n", sqld, sqlc)
	} else {
		t := strings.Replace(table, "/", "_", -1)
		fname := fmt.Sprintf("%s.%s.%s.TABLE.sql", d.Database, schema, t)
		f, err := os.Create(fname)
		checkErr(err)
		defer f.Close()
		f.Write([]byte(sqld))
		f.Write([]byte(sqlc))
	}
	return
}

// GetTable gets table definition conversion
func GetTable(sdb *Database, src Dbase, ddb *Database, dst Dbase, schemaName, tableName string, tbl, lnk, upd, dbg bool) {
	scols, err := sdb.GetColumnDetail(dst, src, schemaName, tableName)
	checkErr(err)
	pcols, err := sdb.GetPKey(dst, src, schemaName, tableName)
	checkErr(err)
	if tbl == false && lnk == false {
		fmt.Println("Table generation not specified")
	} else {
		if tbl {
			td, tc := ddb.GenTable(dst, schemaName, tableName, scols, pcols)
			if dbg {
				fmt.Printf(td + "\n" + tc)
			} else {
				ddb.ExecProcedure(td)
				ddb.ExecProcedure(tc)
			}
		}
		if lnk {
			ld, lc := ddb.GenLink(dst, src, schemaName, tableName, scols, pcols)
			if dbg {
				fmt.Printf(ld + "\n" + lc)
			} else {
				ddb.ExecProcedure(ld)
				ddb.ExecProcedure(lc)
			}
		}
		if upd {
			ud, uc := ddb.GenUpdate(dst, src, schemaName, tableName, scols, pcols)
			if dbg {
				fmt.Printf(ud + "\n" + uc)
			} else {
				ddb.ExecProcedure(ud)
				ddb.ExecProcedure(uc)
			}
		}
	}
}
