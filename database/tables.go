package database

import (
	"fmt"
	"os"
)

//////////
// Tables
//////////

//Table list of tables
type Table struct {
	Name string `db:"TABLE_NAME"`
}

// GetTables returns table list
func (db *Database) GetTables(d Dbase, schemaName string) ([]Table, error) {
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

// GetTable gets table definition
func (db *Database) GetTable(d Dbase, schema, table string) {
	fmt.Printf("\nTABLE: %s.%s", schema, table)
	fname := fmt.Sprintf("%s.%s.%s.TABLE.sql", d.Database, schema, table)
	f, err := os.Create(fname)
	checkErr(err)
	defer f.Close()

	scols, err := db.GetColumnDetail(d, schema, table)
	checkErr(err)
	pcols, err := db.GetPKey(d, schema, table)
	checkErr(err)

	s := db.GenTable(d, schema, table, scols, pcols)
	f.Write([]byte(s))
	return
}
