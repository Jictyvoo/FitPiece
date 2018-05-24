<?php

	namespace Fit_Piece\controllers;

	use Fit_Piece\models\business\ClassDAO;
	use PDO;
	use PDOException;

	class DatabaseController {
		private $connection;
		private $username;
		private $password;

		public function __construct($username, $password, $host, $database_name, $main_SQL_script, $first_CSV_file) {
			$this->connection = new PDO ('mysql:host=' . $host, $username, $password);
			try {
				$this->connection->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
				$this->connection->exec('use ' . $database_name);
			} catch (PDOException $e) {
				/* echo 'ERROR{ ' . $e->getMessage(); */
				$this->executeSQL($main_SQL_script);
				$this->loadCSV($first_CSV_file);
			} finally {
				$this->connection->prepare('CREATE DATABASE IF NOT EXISTS ' . $database_name)->execute();
				$this->connection->prepare('USE ' . $database_name)->execute();
				$userCommand = "CREATE TABLE IF NOT EXISTS User(
				code_user INT NOT NULL AUTO_INCREMENT,
				login VARCHAR(20) NOT NULL,
				password VARCHAR(60) NOT NULL,
				access_level INT,
				PRIMARY KEY(code_user)
			);";
				$this->connection->prepare($userCommand)->execute();
				$this->username = $username;
				$this->password = $password;
			}
		}

		private function loadSQL($sqlFile) {
			$commands = array();
			if ($sqlFile != "") {
				$fileReference = fopen($sqlFile, "r");
				if ($fileReference) {
					$creationLines = [];
					$index = 0;
					while (!feof($fileReference)) {
						$line = str_replace("\n", "", fgets($fileReference)); // apaga o '\n' do final da line
						$creationLines [$index] = $line;
						$index += 1;
					}
					fclose($fileReference);

					$index = 0;
					$tempCommand = "";
					foreach ($creationLines as $value) {
						$tempCommand = $tempCommand . $value . " ";
						$size = strlen($value);
						if ($size > 0) {
							if (count(explode(";", $value)) == 2) {
								$commands [$index] = $tempCommand;
								$tempCommand = "";
								$index += 1;
							}
						}
					}
				}
			}

			return $commands;
		}

		protected function executeSQL($arquivo_sql) {
			$commands = $this->loadSQL($arquivo_sql);
			foreach ($commands as $value) {
				$this->connection->exec($value);
			}
		}

		public function validateUser($username, $password) {
			$command = $this->connection->prepare("SELECT access_level, code_user FROM User WHERE login = '" . $username . "' AND password = '" . $password . "'");
			$command->execute();
			return $command->fetch();
		}

		public function loadCSV($arquivo_csv) {
			if ($arquivo_csv ["file_name"] != "") {
				$file = fopen($arquivo_csv ["file_name"], "r");
				if ($file) {
					while (!feof($file)) {
						$line = explode(";", str_replace("\n", "", fgets($file)));
						if ($line [0] != "") {
							$sqlCommand = "REPLACE " . $arquivo_csv ["associated_table"] . "(" . $arquivo_csv ["table_columns"] . ") VALUES('";
							foreach ($line as $value) {
								$sqlCommand = $sqlCommand . $value;
							}
							$sqlCommand = $sqlCommand . "')";
							$command = $this->connection->prepare($sqlCommand);
							$command->execute();
						}
					}
				}
			}
		}

		private function getTableDescription($tableName) {
			$command = $this->connection->prepare("DESCRIBE " . $tableName);
			$command->execute();
			return $command->fetchall();
		}

		public function generateDAO($tableName) {
			return new ClassDAO ($this->getTableDescription($tableName), $tableName, $this->connection);
		}

		public function execute($command) {
			$execution = $this->connection->prepare($command);
			$execution->execute();
		}
	}
