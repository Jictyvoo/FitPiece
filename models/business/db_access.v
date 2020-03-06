module database_connection

import mysql
import os
#flag -I/usr/include/mysql

struct DatabaseAccess {
	connection mysql.DB
	dao_classes map[string]ClassDao
}

fn (db mut DatabaseAccess) table_exists(table_name string) bool {
	tables := db.connection.query("SHOW TABLES") or { eprintln(err) return false }
	for row in tables.rows() { if row.vals[0] == table_name {return true} }
	return false
}

pub fn (db mut DatabaseAccess) generate_dao(table_name string) ?ClassDao {
	if !table_name in db.dao_classes {
		mut table_columns := [[]string]
		mut exist := true
		if db.table_exists(table_name) {
			table_description := db.connection.query("DESCRIBE $table_name") or {eprintln(err) return none}
			for columns in table_description.rows() {table_columns << columns.vals}
		} else { exist = false }
		db.dao_classes[table_name] = ClassDao {table_name: table_name, connection: db.connection, columns: table_columns}
		if !exist {
			db.dao_classes[table_name].create() or {return error(err)}
		}
	}
	return db.dao_classes[table_name]
}

pub fn (db mut DatabaseAccess) create_table(table_name string, column_names []string, column_datatypes []string) ?ClassDao {
	if !db.table_exists(table_name) {
		mut sql_command := "CREATE TABLE $table_name ("
		for count := 0; count < column_names.len; count += 1 {
			if count > 0 {sql_command += ","}
			column_name := column_names[count]
			datatype := column_datatypes[count]
			sql_command += " $column_name $datatype"
		}
		sql_command += ");"
		db.connection.query(sql_command) or {return error(err)}
	}
	generated_dao := db.generate_dao(table_name) or {return error(err)}
	return generated_dao
}

pub fn create_connection(server string, user string, passwd string, dbname string) ?DatabaseAccess {
	conn := mysql.connect(server, user, passwd, dbname) or {return error(err)}
    db := DatabaseAccess{connection: conn}
    return db
}

pub fn (db DatabaseAccess) get_connection() mysql.DB {
	return db.connection
}

pub fn (db DatabaseAccess) end() {
	db.connection.close()
}

fn (db DatabaseAccess) load_sql(filename string) []string {
	mut commands := []string
	if filename != "" && os.exists(filename) {
		file_lines := os.read_lines(filename)
		creation_lines = []string
		for line in file_lines {
			creation_lines << line.replace("\n", "") // erase '\n' at the line end
		}

		mut index := 0;
		mut temp_command := "";
		for value in creation_lines {
			temp_command += value + " "
			if value.len > 0 {
				if value.split(";").len == 2 {
					commands << temp_command
					temp_command = ""
				}
			}
		}
	}

	return commands
}

pub fn (db DatabaseAccess) execute_sql(sql_file string) bool {
	commands := db.load_sql(sql_file)
	for value in commands {
		db.connection.query(value) or {
			return false
		}
	}
	return true
}

/*
	On csv_file array, position:
	0 - filename
	1 - associated_table
	2 - table_columns
*/
pub fn (db DatabaseAccess) load_csv(csv_file []string) bool {
	if csv_file[0] != "" {
		if os.exists(csv_file[0]) {
			for line in os.read_lines(csv_file[0]) {
				data := line.replace("\n", "").split(";")
				if line [0] != "" {
					mut sql_command := "REPLACE " + csv_file[1] + "(" + csv_file[2] + ") VALUES('"
					for value in line {
						sql_command = sql_command + value
					}
					sql_command = sql_command + "')";
					command = db.connection.query(sql_command) or {
						return false
					}
					return true
				}
			}
		}
	}
	return false
}

pub fn (db DatabaseAccess) execute(command) bool {
	db.connection.query(command) or {
		return false
	}
	return true
}
