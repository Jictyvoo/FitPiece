<?php
class DatabaseController {
	private $connection;
	private $username;
	private $password;
	private $LoadInDatabase;
	public function __construct($username, $password, $host, $database_name, $main_SQL_script, $first_CSV_file) {
		$this->connection = new PDO ( 'mysql:host='.$host, $username, $password );
		try {
			$this->connection->setAttribute ( PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION );
			$this->connection->exec ( 'use ' . $database_name );
		} catch ( PDOException $e ) {
			/* echo 'ERROR{ ' . $e->getMessage(); */
			$this->executarSQL ( $main_SQL_script );
			$userCommand = "create table if not exists User(
				code_user int not null auto_increment,
				login varchar(20) not null,
				password varchar(30) not null,
				acces_level int,
				primary key(code_user)
			);";
			$this ->connection -> exec($userCommand);
			$this->loadCSV ( $first_CSV_file );
		} finally{
			$this->username = $username;
			$this->password = $password;
		}
	}
	private function carregarSQL($arquivo_sql) {
		$comandos = array();
		if($arquivo_sql != ""){
			$ref_arquivo = fopen ( $arquivo_sql, "r" );
			
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
		
		return $comandos;
	}
	protected function executarSQL($arquivo_sql) {
		$comandos = $this->carregarSQL ( $arquivo_sql );
		foreach ( $comandos as $value ) {
			$this->connection->exec ( $value );
		}
	}
	public function validateUser($username, $password) {
		$command = $this->connection->prepare ( "SELECT acces_level FROM User WHERE login = '" . $username . "' and password = '" . $password . "'" );
		$command->execute ();
		return $command->fetchColumn ();
	}

	public function loadCSV($arquivo_csv) {
		if($arquivo_csv["file_name"] != ""){
			$file = fopen ( $arquivo_csv, "r" );
			
			while ( ! feof ( $file ) ) {
				$line = explode ( ";", str_replace ( "\n", "", fgets ( $file ) ) ); // apaga o '\n' do final da linha
				if ($line [0] != "") {
					$sqlCommand = "REPLACE " . $arquivo_csv["associated_table"] . "(". $arquivo_csv["table_columns"] . ") VALUES('" . $line [0] . "')";
					$command = $this->connection->prepare ( $sqlCommand );
					$command->execute ();
				}
			}
		}
	}
}

?>