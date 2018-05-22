<?php
$amount = "";
$path = explode ( "/", $_SERVER ['SCRIPT_FILENAME'] );
$rootPathArray = explode ( "/", $_SERVER ['DOCUMENT_ROOT'] );
$rootPath = $rootPathArray [count ( $rootPathArray ) - 1];
for($index = count ( $path ); $index >= 0; $index -= 1) {
	if ($index >= count ( $path ) - 1) {
		continue;
	} else if ($path [$index] == $rootPath) {
		break;
	}
	$amount = $amount . "../";
}
require_once ($amount . "models/business/ClassDAO.php");
class DatabaseController {
	private $connection;
	private $username;
	private $password;
	public function __construct($username, $password, $host, $database_name, $main_SQL_script, $first_CSV_file) {
		$this->connection = new PDO ( 'mysql:host=' . $host, $username, $password );
		try {
			$this->connection->setAttribute ( PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION );
			$this->connection->exec ( 'use ' . $database_name );
		} catch ( PDOException $e ) {
			/* echo 'ERROR{ ' . $e->getMessage(); */
			$this->executeSQL ( $main_SQL_script );
			$this->loadCSV ( $first_CSV_file );
		} finally {
			$this->connection->exec ( 'CREATE DATABASE IF NOT EXISTS ' . $database_name );
			$userCommand = "CREATE TABLE IF NOT EXISTS User(
				code_user int not null auto_increment,
				login varchar(20) not null,
				password varchar(60) not null,
				access_level int,
				primary key(code_user)
			);";
			$this->connection->exec ( $userCommand );
			$this->username = $username;
			$this->password = $password;
		}
	}
	private function loadSQL($arquivo_sql) {
		$comandos = array ();
		if ($arquivo_sql != "") {
			$ref_arquivo = fopen ( $arquivo_sql, "r" );
			if ($ref_arquivo) {
				$index = 0;
				while ( ! feof ( $ref_arquivo ) ) {
					$linha = str_replace ( "\n", "", fgets ( $ref_arquivo ) ); // apaga o '\n' do final da linha
					$linhasCriacao [$index] = $linha;
					$index += 1;
				}
				fclose ( $ref_arquivo );
				
				$index = 0;
				$tempComando = "";
				foreach ( $linhasCriacao as $value ) {
					$tempComando = $tempComando . $value . " ";
					$tamanho = strlen ( $value );
					if ($tamanho > 0) {
						if (count ( explode ( ";", $value ) ) == 2) {
							$comandos [$index] = $tempComando;
							$tempComando = "";
							$index += 1;
						}
					}
				}
			}
		}
		
		return $comandos;
	}
	protected function executeSQL($arquivo_sql) {
		$comandos = $this->loadSQL ( $arquivo_sql );
		foreach ( $comandos as $value ) {
			$this->connection->exec ( $value );
		}
	}
	public function validateUser($username, $password) {
		$command = $this->connection->prepare ( "SELECT access_level, code_user FROM User WHERE login = '" . $username . "' and password = '" . $password . "'" );
		$command->execute ();
		return $command->fetch ();
	}
	public function loadCSV($arquivo_csv) {
		if ($arquivo_csv ["file_name"] != "") {
			$file = fopen ( $arquivo_csv ["file_name"], "r" );
			if ($file) {
				while ( ! feof ( $file ) ) {
					$line = explode ( ";", str_replace ( "\n", "", fgets ( $file ) ) );
					if ($line [0] != "") {
						$sqlCommand = "REPLACE " . $arquivo_csv ["associated_table"] . "(" . $arquivo_csv ["table_columns"] . ") VALUES('";
						foreach ( $line as $value ) {
							$sqlCommand = $sqlCommand . $value;
						}
						$sqlCommand = $sqlCommand . "')";
						$command = $this->connection->prepare ( $sqlCommand );
						$command->execute ();
					}
				}
			}
		}
	}
	private function getTableDescription($tableName) {
		$command = $this->connection->prepare ( "DESCRIBE " . $tableName );
		$command->execute ();
		return $command->fetchall ();
	}
	public function generateDAO($tableName) {
		return new ClassDAO ( $this->getTableDescription ( $tableName ), $tableName, $this->connection );
	}
	public function execute($command) {
		$execution = $this->connection->prepare ( $command );
		$execution->execute ();
	}
}
