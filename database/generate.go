package database

import (
	"fmt"
	"strings"
)

//GenTable generate table craeation
func (db *Database) GenTable(conn *Conn, table string, cols []Column, pkey []PKey) (sqld, sqlc string) {
	clen := len(cols)
	plen := len(pkey)
	if conn.Source.Driver == "postgres" {
		sqld += fmt.Sprintf("\nDROP TABLE IF EXISTS \"%s\".\"%s\" CASCADE;", conn.SSchema, table)
		sqlc += fmt.Sprintf("\nCREATE TABLE IF NOT EXISTS \"%s\".\"%s\" (\n", conn.SSchema, table)
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
		sqlc += ");\n"
	} else if conn.Source.Driver == "mssql" {
		sqld += fmt.Sprintf("\nDROP TABLE \"%s\".\"%s\";", conn.SSchema, table)
		sqlc += fmt.Sprintf("\nCREATE TABLE \"%s\".\"%s\" (\n", conn.SSchema, table)
		for k, c := range cols {
			//fmt.Println(c)
			if k == clen-1 {
				if plen > 0 {
					sqlc += fmt.Sprintf("%s,\n", c.Column)
					sqlc += "PRIMARY KEY ("
					for v, p := range pkey {
						if v == plen-1 {
							sqlc += fmt.Sprintf("\"%s\"", p.PKey)
						} else {
							sqlc += fmt.Sprintf("\"%s\",", p.PKey)
						}
					}
					sqlc += ")\n"
				} else {
					// q += c.Column + "\n"
					sqlc += fmt.Sprintf("%s\n", c.Column)
				}
			} else {
				sqlc += fmt.Sprintf("%s,\n", c.Column)
			}
		}
		sqlc += ")\n"
	}
	return
}

//GenCopyTable generate table craeation
func (db *Database) GenCopyTable(src Database, dst Database, sschema, dschema, t string, cols []Column, pkey []PKey) (sqld, sqlc string) {
	// fmt.Println(dst.Driver, s, t, pkey)
	clen := len(cols)
	plen := len(pkey)
	if dst.Driver == "postgres" {
		sqld += fmt.Sprintf("\nDROP TABLE IF EXISTS \"%s\".\"%s\" CASCADE;\n", dschema, t)
		sqlc += fmt.Sprintf("CREATE TABLE IF NOT EXISTS \"%s\".\"%s\" (\n", dschema, t)
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
		sqlc += ");\n"
	} else if dst.Driver == "mssql" {
		sqld += fmt.Sprintf("\nDROP TABLE \"%s\".\"%s\";\n", dschema, t)
		sqlc += fmt.Sprintf("CREATE TABLE \"%s\".\"%s\" (\n", dschema, t)
		for k, c := range cols {
			//fmt.Println(c)
			if k == clen-1 {
				if plen > 0 {
					sqlc += fmt.Sprintf("%s,\n", c.Column)
					sqlc += "PRIMARY KEY ("
					for v, p := range pkey {
						if v == plen-1 {
							sqlc += fmt.Sprintf("\"%s\"", p.PKey)
						} else {
							sqlc += fmt.Sprintf("\"%s\",", p.PKey)
						}
					}
					sqlc += ")\n"
				} else {
					// q += c.Column + "\n"
					sqlc += fmt.Sprintf("%s\n", c.Column)
				}
			} else {
				sqlc += fmt.Sprintf("%s,\n", c.Column)
			}
		}
		sqlc += ")\n"
	}
	return
}

//GenLink generate table creation
func (db *Database) GenLink(conn *Conn, table string, cols []Column, pkey []PKey) (sqld, sqlc string) {
	tmp := ""
	if table == strings.ToUpper(table) {
		tmp = "TEMP"
	} else {
		tmp = "temp"
	}
	clen := len(cols)
	if conn.Dest.Driver == "postgres" {
		sqld += fmt.Sprintf("\nDROP FOREIGN TABLE IF EXISTS \"%s\".\"%s%s\" CASCADE;\n", conn.DSchema, table, tmp)
		sqlc += fmt.Sprintf("CREATE FOREIGN TABLE IF NOT EXISTS \"%s\".\"%s%s\" (\n", conn.DSchema, table, tmp)
		for k, c := range cols {
			if k == clen-1 {
				sqlc += fmt.Sprintf("%s\n", c.Column)
			} else {
				sqlc += fmt.Sprintf("%s,\n", c.Column)
			}
		}
		sqlc += ")\n"
		sqlc += fmt.Sprintf("SERVER %s \nOPTIONS (", conn.Source.Name)
		sqlc += fmt.Sprintf("table_name '%s.%s', ", conn.SSchema, table)
		sqlc += "row_estimate_method 'showplan_all', "
		sqlc += "match_column_names '0');\n"
	} else if conn.Dest.Driver == "mssql" {
		sqld += fmt.Sprintf("\nDROP VIEW \"%s\".\"%s%s\";\n", conn.DSchema, table, tmp)
		sqlc += fmt.Sprintf("CREATE VIEW \"%s\".\"%s%s\" AS\nSELECT\n", conn.DSchema, table, tmp)
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
		sqlc += fmt.Sprintf("FROM \"%s\".\"%s\".\"%s\".\"%s\";\n", conn.Source.Host, conn.Source.Database, conn.SSchema, table)
	}
	return sqld, sqlc
}

//GenUpdate generate update procedure
func (db *Database) GenUpdate(conn *Conn, table string, cols []Column, pkey []PKey) (sqld, sqlc string) {
	columns := trimCols(cols, pkey)
	// fmt.Println(cols)

	sqld, sqlc = tableUpdProcStart(conn.Dest.Driver, conn.DSchema, table)
	sqlc += tableDeleteSQL(conn.Dest.Driver, conn.DSchema, table, pkey, cols)
	if len(pkey) != len(cols) {
		sqlc += tableUpdateSQL(conn.Dest.Driver, conn.DSchema, table, pkey, columns)
	}
	sqlc += tableInsertSQL(conn.Dest.Driver, conn.DSchema, table, pkey, cols)
	sqlc += tableUpdProcEnd(conn.Dest.Driver, table)
	return
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
		sqld += fmt.Sprintf("\nDROP PROCEDURE IF EXISTS \"%s\".\"upd_%s\"();", schema, tableName)
		sqlc += fmt.Sprintf("\nCREATE OR REPLACE PROCEDURE \"%s\".\"upd_%s\"()\nLANGUAGE plpgsql\nAS $procedure$\nBEGIN\n", schema, tableName)
	} else if destDriver == "mssql" {
		sqld += fmt.Sprintf("\nDROP PROCEDURE \"%s\".\"upd_%s\";", schema, tableName)
		sqlc += fmt.Sprintf("\nCREATE PROCEDURE \"%s\".\"upd_%s\" AS\nBEGIN\n", schema, tableName)
		sqlc += fmt.Sprintf("IF OBJECT_ID('tempdb..#%s','U') IS NOT NULL DROP TABLE tempdb.#%s\n", tableName, tableName)
		sqlc += fmt.Sprintf("SELECT * INTO #%s FROM \"%s\".\"%s%s\"\n", tableName, schema, tableName, tmp)
	}
	return sqld, sqlc
}

func tableUpdProcEnd(destDriver, tableName string) (sqlc string) {
	if destDriver == "postgres" {
		sqlc += "END\n$procedure$;\n"
	} else if destDriver == "mssql" {
		sqlc += fmt.Sprintf("IF OBJECT_ID('tempdb..#%s','U') IS NOT NULL DROP TABLE tempdb.#%s\n", tableName, tableName)
		sqlc += "END;\n"
	}
	return sqlc
}

// func tableIndexSQL(destDriver, tableName string, pkey []PKey) (sqlc string) {
// 	if destDriver == "postgres" {
// 		sqlc += fmt.Sprintf("CREATE INDEX \"tp_%s\" ON \"temp_%s\" (", tableName, tableName)
// 	} else if destDriver == "mssql" {
// 		sqlc += fmt.Sprintf("CREATE INDEX \"tp_%s\" ON #%s (", tableName, tableName)
// 	}
// 	plen := len(pkey)
// 	for k, p := range pkey {
// 		sqlc += fmt.Sprintf("\"%s\"", p.PKey)
// 		if k == plen-1 {
// 			sqlc += ""
// 		} else {
// 			sqlc += ","
// 		}
// 	}
// 	if destDriver == "postgres" {
// 		sqlc += ");\n"
// 	} else if destDriver == "mssql" {
// 		sqlc += ")\n"
// 	}
// 	return sqlc
// }

func tableDeleteSQL(destDriver, schema, tableName string, pkey []PKey, allColumns []Column) (sqlc string) {
	ttemp := "temp"
	if schema == "ep1" {
		ttemp = "TEMP"
	}
	if destDriver == "postgres" {
		sqlc += "DELETE\n"
	} else if destDriver == "mssql" {
		sqlc += fmt.Sprintf("DELETE \"%s\".\"%s\"\n", schema, tableName)
	}
	sqlc += fmt.Sprintf("FROM \"%s\".\"%s\"\n", schema, tableName)
	if destDriver == "postgres" {
		sqlc += fmt.Sprintf("USING \"%s\".\"%s\" AS d\n", schema, tableName)
		//sqlc += fmt.Sprintf("LEFT OUTER JOIN \"temp_%s\" \"%stemp\" ON", tableName, tableName)

		sqlc += fmt.Sprintf("LEFT OUTER JOIN \"%s\".\"%s%s\" \"%s%s\" ON", schema, tableName, ttemp, tableName, ttemp)
	} else if destDriver == "mssql" {
		sqlc += fmt.Sprintf("LEFT JOIN #%s \"%s%s\" ON", tableName, ttemp, tableName)
	}
	plen := len(pkey)
	for k, p := range pkey {
		if destDriver == "postgres" {
			sqlc += fmt.Sprintf("\nd.\"%s\" = \"%s%s\".\"%s\"", p.PKey, tableName, ttemp, p.PKey)
		} else if destDriver == "mssql" {
			sqlc += fmt.Sprintf("\n\"%s\".\"%s\" = \"%s%s\".\"%s\"", tableName, p.PKey, tableName, ttemp, p.PKey)
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
		sqlc += fmt.Sprintf("AND \"%s%s\".\"%s\" IS NULL;\n", tableName, ttemp, allColumns[0].ColumnName)

	} else if destDriver == "mssql" {
		sqlc += fmt.Sprintf("WHERE \"%s%s\".\"%s\" IS NULL\n", tableName, ttemp, allColumns[0].ColumnName)
	}
	return sqlc
}

func tableUpdateSQL(destDriver, schema, tableName string, pkey []PKey, columns []Column) (sqlc string) {
	ttemp := "temp"
	if schema == "ep1" {
		ttemp = "TEMP"
	}

	sqlc += fmt.Sprintf("UPDATE \"%s\".\"%s\"\nSET", schema, tableName)
	plen := len(pkey)
	clen := len(columns)
	for k, c := range columns {
		sqlc += fmt.Sprintf("\n\"%s\" = \"%s%s\".\"%s\"", c.ColumnName, tableName, ttemp, c.ColumnName)
		if k == clen-1 {
			sqlc += ""
		} else {
			sqlc += ","
		}
	}
	if destDriver == "postgres" {
		sqlc += fmt.Sprintf("\nFROM \"%s\".\"%s%s\" \"%s%s\"", schema, tableName, ttemp, tableName, ttemp)
	} else if destDriver == "mssql" {
		sqlc += fmt.Sprintf("\nFROM #%s \"%s%s\"", tableName, ttemp, tableName)
	}
	if destDriver == "postgres" {
		sqlc += "\nWHERE"
	} else if destDriver == "mssql" {
		sqlc += fmt.Sprintf("\nJOIN \"%s\".\"%s\" ON", schema, tableName)
	}
	for k, p := range pkey {
		sqlc += fmt.Sprintf("\n\"%s\".\"%s\" = \"%s%s\".\"%s\"", tableName, p.PKey, tableName, ttemp, p.PKey)
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
		sqlc += fmt.Sprintf("\n\"%s\".\"%s\" <> \"%s%s\".\"%s\"", tableName, c.ColumnName, tableName, ttemp, c.ColumnName)
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
	ttemp := "temp"
	if schema == "ep1" {
		ttemp = "TEMP"
	}

	plen := len(pkey)
	clen := len(allColumns)
	sqlc += fmt.Sprintf("INSERT INTO \"%s\".\"%s\"\n", schema, tableName)
	sqlc += "SELECT"
	for k, c := range allColumns {
		sqlc += fmt.Sprintf("\n\"%s%s\".\"%s\" \"%s\"", tableName, ttemp, c.ColumnName, c.ColumnName)
		if k == clen-1 {
			sqlc += "\n"
		} else {
			sqlc += ","
		}
	}
	sqlc += fmt.Sprintf("FROM \"%s\".\"%s\"\n", schema, tableName)
	if destDriver == "postgres" {
		sqlc += fmt.Sprintf("RIGHT JOIN \"%s\".\"%s%s\" \"%s%s\" ON", schema, tableName, ttemp, tableName, ttemp)
	} else if destDriver == "mssql" {
		sqlc += fmt.Sprintf("RIGHT JOIN #%s \"%s%s\" ON", tableName, tableName, ttemp)
	}
	for k, p := range pkey {
		sqlc += fmt.Sprintf("\n\"%s\".\"%s\" = \"%s%s\".\"%s\"", tableName, p.PKey, tableName, ttemp, p.PKey)
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

// func tableUpsertSQL(destDriver, schema, tableName string, pkey []PKey, allColumns []Column, columns []Column) (sqlc string) {
// 	plen := len(pkey)
// 	clen := len(allColumns)
// 	sqlc += fmt.Sprintf("INSERT INTO \"%s\".\"%s\"\n", schema, tableName)
// 	sqlc += "SELECT"
// 	for k, c := range allColumns {
// 		sqlc += fmt.Sprintf("\n\"%sTEMP\".\"%s\" \"%s\"", tableName, c.ColumnName, c.ColumnName)
// 		if k == clen-1 {
// 			sqlc += "\n"
// 		} else {
// 			sqlc += ","
// 		}
// 	}
// 	sqlc += fmt.Sprintf("FROM \"%s\".\"%s\"\n", schema, tableName)
// 	if destDriver == "postgres" {
// 		//sqlc += fmt.Sprintf("RIGHT JOIN \"temp_%s\" \"%stemp\" ON", tableName, tableName)
// 		sqlc += fmt.Sprintf("RIGHT JOIN \"%s\".\"%sTEMP\" \"%sTEMP\" ON", schema, tableName, tableName)
// 	} else if destDriver == "mssql" {
// 		sqlc += fmt.Sprintf("RIGHT JOIN #%s \"%stemp\" ON", tableName, tableName)
// 	}
// 	for k, p := range pkey {
// 		sqlc += fmt.Sprintf("\n\"%s\".\"%s\" = \"%sTEMP\".\"%s\"", tableName, p.PKey, tableName, p.PKey)
// 		if k == plen-1 {
// 			sqlc += "\n"
// 		} else {
// 			sqlc += " AND "
// 		}
// 	}

// 	if destDriver == "postgres" {
// 		sqlc += fmt.Sprintf("WHERE \"%s\".\"%s\" IS NULL\n", tableName, allColumns[0].ColumnName)
// 		if clen == plen {
// 			sqlc += "ON CONFLICT DO NOTHING;\n"
// 		} else {
// 			sqlc += "ON CONFLICT("
// 			for i, p := range pkey {
// 				if i+1 < plen {
// 					sqlc += fmt.Sprintf("\"%s\",", p.PKey)
// 				} else {
// 					sqlc += fmt.Sprintf("\"%s\"", p.PKey)
// 				}
// 			}
// 			sqlc += ")\n"
// 			sqlc += "DO UPDATE SET"
// 			colslen := len(columns)
// 			for k, c := range columns {
// 				sqlc += fmt.Sprintf("\n\"%s\" = EXCLUDED.\"%s\"", c.ColumnName, c.ColumnName)
// 				if k == colslen-1 {
// 					sqlc += ";\n"
// 				} else {
// 					sqlc += ", "
// 				}
// 			}

// 		}
// 	} else if destDriver == "mssql" {
// 		sqlc += fmt.Sprintf("WHERE \"%s\".\"%s\" IS NULL\n", tableName, allColumns[0].ColumnName)
// 	}
// 	return sqlc
// }
