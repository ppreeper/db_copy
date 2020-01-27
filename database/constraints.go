package database

import "fmt"

//PKey struct
type PKey struct {
	PKey string `db:"CL"`
}

//GetPKey func
func (db *Database) GetPKey(dst Dbase, src Dbase, s, t string) ([]PKey, error) {
	q := ""
	if src.Driver == "" {
		src = dst
	}
	if src.Driver == "postgres" {
		q += fmt.Sprintf("SELECT C.COLUMN_NAME \"CL\"")
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
		q += fmt.Sprintf("SELECT C.COLUMN_NAME \"CL\"")
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
	var pkey []PKey
	if err := db.Select(&pkey, q); err != nil {
		return nil, fmt.Errorf("Select: %v", err)
	}
	return pkey, nil
}
