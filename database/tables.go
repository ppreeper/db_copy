package database

import (
	"fmt"
)

//////////
// Tables
//////////

//Table list of tables
type Table struct {
	Name string `db:"TABLE_NAME"`
}

// GetTableList returns table list
func (db *Database) GetTables(schemaName string) ([]Table, error) {
	q := ""
	if db.Driver == "postgres" || db.Driver == "mssql" {
		q += "select TABLE_NAME \"TABLE_NAME\" "
		q += "from INFORMATION_SCHEMA.TABLES "
		q += "where TABLE_SCHEMA = '" + schemaName + "' "
		q += "and TABLE_TYPE = 'BASE TABLE' "
		q += "order by TABLE_NAME"
	}
	tt := []Table{}
	if err := db.Select(&tt, q); err != nil {
		return nil, fmt.Errorf("select: %w", err)
	}
	return tt, nil
}

// GetTableSchema gets table definition
func (db *Database) GetTableSchema(conn *Conn, table string) (sqld, sqlc string) {
	scols, err := db.GetColumnDetail(conn, table, false)
	db.checkErr(err)
	pcols, err := db.GetPKey(conn, table)
	db.checkErr(err)
	sqld, sqlc = db.GenTable(conn, table, scols, pcols)
	return
}

// GetForeignTableSchema gets table definition
func (db *Database) GetForeignTableSchema(conn *Conn, table string) (sqld, sqlc string) {
	scols, err := db.GetColumnDetail(conn, table, false)
	db.checkErr(err)
	pcols, err := db.GetPKey(conn, table)
	db.checkErr(err)
	sqld, sqlc = db.GenLink(conn, table, scols, pcols)
	return
}

// GetUpdateTableSchema gets table definition
func (db *Database) GetUpdateTableSchema(conn *Conn, table string) (sqld, sqlc string) {
	scols, err := db.GetColumnDetail(conn, table, false)
	db.checkErr(err)
	pcols, err := db.GetPKey(conn, table)
	db.checkErr(err)
	sqld, sqlc = db.GenUpdate(conn, table, scols, pcols)
	return
}
