package database

import "fmt"

//PKey struct
type PKey struct {
	PKey string `db:"CL"`
}

//GetPKey func
func (db *Database) GetPKey(conn *Conn, table string) ([]PKey, error) {
	q := ""
	// if src.Driver == "" {
	// 	src = dst
	// }
	if conn.Source.Driver == "postgres" || conn.Source.Driver == "mssql" {
		q += "SELECT C.COLUMN_NAME \"CL\""
		q += fmt.Sprintf("\nFROM %s.INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE C", conn.Source.Database)
		q += fmt.Sprintf("\nJOIN %s.INFORMATION_SCHEMA.COLUMNS CLM ON", conn.Source.Database)
		q += "\nC.TABLE_CATALOG = CLM.TABLE_CATALOG AND "
		q += "\nC.TABLE_SCHEMA = CLM.TABLE_SCHEMA AND "
		q += "\nC.TABLE_NAME = CLM.TABLE_NAME AND "
		q += "\nC.COLUMN_NAME = CLM.COLUMN_NAME"
		q += fmt.Sprintf("\nWHERE C.TABLE_CATALOG = '%s'", conn.Source.Database)
		q += fmt.Sprintf("\nAND C.TABLE_SCHEMA = '%s'", conn.SSchema)
		q += fmt.Sprintf("\nAND C.TABLE_NAME IN ('%s')", table)
		q += "\nAND C.CONSTRAINT_NAME IN ("
		q += "\nSELECT CONSTRAINT_NAME"
		q += fmt.Sprintf("\nFROM %s.INFORMATION_SCHEMA.TABLE_CONSTRAINTS C", conn.Source.Database)
		q += fmt.Sprintf("\nWHERE C.TABLE_CATALOG = '%s'", conn.Source.Database)
		q += fmt.Sprintf("\nAND C.TABLE_SCHEMA = '%s'", conn.SSchema)
		q += "\nAND CONSTRAINT_TYPE = 'PRIMARY KEY'"
		q += fmt.Sprintf("\nAND C.TABLE_NAME IN ('%s')", table)
		q += "\n)"
		q += "\nORDER BY CLM.ORDINAL_POSITION"
	}
	// fmt.Println(q)
	var pkey []PKey
	if err := db.Select(&pkey, q); err != nil {
		return nil, fmt.Errorf("select: %w", err)
	}
	return pkey, nil
}
