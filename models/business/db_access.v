module database_connection
import mysql
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

pub fn (db DatabaseAccess) loadSQL(sqlFile) {
	commands = []string
	if (sqlFile != "") {
		fileReference = fopen(sqlFile, "r");
		if (fileReference) {
			creationLines = []string
			index = 0;
			while (!feof(fileReference)) {
				line = str_replace("\n", "", fgets(fileReference)); // erase '\n' at the line end
				creationLines [index] = line;
				index += 1;
			}
			fclose(fileReference);

			index = 0;
			tempCommand = "";
			foreach (creationLines as value) {
				tempCommand = tempCommand . value . " ";
				size = strlen(value);
				if (size > 0) {
					if (count(explode(";", value)) == 2) {
						commands [index] = tempCommand;
						tempCommand = "";
						index += 1;
					}
				}
			}
		}
	}

	return commands;
}

protected function executeSQL(arquivo_sql) {
	commands = this->loadSQL(arquivo_sql);
	foreach (commands as value) {
		this->connection->exec(value);
	}
}

public function loadCSV(arquivo_csv) {
	if (arquivo_csv ["file_name"] != "") {
		file = fopen(arquivo_csv ["file_name"], "r");
		if (file) {
			while (!feof(file)) {
				line = explode(";", str_replace("\n", "", fgets(file)));
				if (line [0] != "") {
					sqlCommand = "REPLACE " . arquivo_csv ["associated_table"] . "(" . arquivo_csv ["table_columns"] . ") VALUES('";
					foreach (line as value) {
						sqlCommand = sqlCommand . value;
					}
					sqlCommand = sqlCommand . "')";
					command = this->connection->prepare(sqlCommand);
					command->execute();
				}
			}
		}
	}
}

public function execute(command) {
	execution = this->connection->prepare(command);
	execution->execute();
}
