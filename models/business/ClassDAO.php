<?php

	namespace Fit_Piece\models\business;

	use Fit_Piece\Exception\UnsupportedOperationException;

	class ClassDAO {
		private $columns;
		private $tableName;
		private $databaseConnection;

		public function __construct($tableDescription, $tableName, $databaseConnection) {
			$this->tableName = $tableName;
			$this->databaseConnection = $databaseConnection;
			$this->columns = $tableDescription;
		}

		private function commandCreation($sqlCommand, $count, $value, $delimiter) {
			if ($count > 0) {
				$sqlCommand = $sqlCommand . $delimiter;
			}
			$sqlCommand = $sqlCommand . $value;
			return $sqlCommand;
		}

		private function arrayToValueString($valuesArray, $delimiter) {
			$valuesCommand = "";
			if (count($valuesArray) < (count($this->columns))) {
				$count = -1;
			} else {
				$count = 0;
			}
			foreach ($this->columns as $key => $value) {
				if ($count == -1 || $count == count($valuesArray)) {
					$count += 1;
					continue;
				}
				if ($valuesArray [$count] == null && $value ["Null"] == "No") {
					return null;
				}
				$valuesCommand = $this->commandCreation($valuesCommand, $count, $valuesArray [$count], $delimiter);
				$count += 1;
			}
			return $valuesCommand;
		}

		private function generateColumnFields($valuesArray, $delimiter) {
			$sqlCommand = "";
			if (count($valuesArray) < (count($this->columns))) {
				$count = -1;
			} else {
				$count = 0;
			}
			foreach ($this->columns as $key => $value) {
				if ($count == -1 || $count == count($valuesArray)) {
					$count += 1;
					continue;
				}
				$sqlCommand = $this->commandCreation($sqlCommand, $count, $value ["Field"], $delimiter);
				$count += 1;
			}
			return $sqlCommand;
		}

		private function isAssociative(array $testArray) {
			if (array() === $testArray) return false;
			return array_keys($testArray) !== range(0, count($testArray) - 1);
		}

		private function generateAssociative($valuesArray, $delimiter) {
			$columnsString = "";
			$valuesString = "";
			foreach ($valuesArray as $key => $value) {
				if (strlen($columnsString) > 0) {
					$columnsString = $columnsString . $delimiter;
					$valuesString = $valuesString . $delimiter;
				}
				if (is_numeric($key)) {
					throw new UnsupportedOperationException ("Associative Array can only have string Keys");
				}
				$columnsString = $columnsString . $key;
				$valuesString = $valuesString . $value;
			}
			return array($columnsString, $valuesString);
		}

		private function insertAssociative($valuesArray) {
			$generatedAssociative = $this->generateAssociative($valuesArray, ", ");
			$columnsString = $generatedAssociative [0];
			$valuesString = $generatedAssociative [1];
			$sqlCommand = "INSERT INTO " . $this->tableName . "( " . $columnsString . ") VALUES(" . $valuesString . ")";
			return $sqlCommand;
		}

		private function insertCommon($valuesArray) {
			$sqlCommand = "INSERT INTO " . $this->tableName . "(";
			$sqlCommand = $sqlCommand . $this->generateColumnFields($valuesArray, ", ");
			$sqlCommand = $sqlCommand . ") VALUES(";
			$sqlCommand = $sqlCommand . $this->arrayToValueString($valuesArray, ", ") . ")";
			return $sqlCommand;
		}

		public function insert($valuesArray) {
			if ($this->isAssociative($valuesArray)) {
				$sqlCommand = $this->insertAssociative($valuesArray);
			} else {
				$sqlCommand = $this->insertCommon($valuesArray);
			}
			$command = $this->databaseConnection->prepare($sqlCommand);
			$command->execute();
		}

		private function valuePerColumn($values, $columns, $sqlCommand, $comparison) {
			$sqlCommand = $sqlCommand ? $sqlCommand : "";
			for ($index = 0; $index < count($values); $index += 1) {
				if ($index > 0) {
					$sqlCommand = $sqlCommand . ", ";
				}
				$sqlCommand = $sqlCommand . $columns [$index] . $comparison . $values [$index];
			}
			return $sqlCommand;
		}

		private function updateCommon($valuesArray) {
			$sqlCommand = "UPDATE " . $this->tableName . " SET ";
			$values = explode(";", $this->arrayToValueString($valuesArray, ";"));
			$columns = explode(";", $this->generateColumnFields($valuesArray, ";"));
			$sqlCommand = $this->valuePerColumn($values, $columns, $sqlCommand, " = ");
			return $sqlCommand;
		}

		private function updateAssociative($valuesArray) {
			$sqlCommand = "UPDATE " . $this->tableName . " SET ";
			$generatedAssociative = $this->generateAssociative($valuesArray, ";");
			$columns = explode(";", $generatedAssociative [0]);
			$values = explode(";", $generatedAssociative [1]);
			$sqlCommand = $this->valuePerColumn($values, $columns, $sqlCommand, " = ");
			return $sqlCommand;
		}

		public function update($valuesArray, $whereCommand) {
			if ($this->isAssociative($valuesArray)) {
				$sqlCommand = $this->updateAssociative($valuesArray);
			} else {
				$sqlCommand = $this->updateCommon($valuesArray);
			}
			if ($whereCommand) {
				$sqlCommand = $sqlCommand . " WHERE " . $whereCommand;
			}
			$command = $this->databaseConnection->prepare($sqlCommand);
			$command->execute();
		}

		public function delete($condition) {
			if ($condition) {
				$sqlCommand = "DELETE FROM " . $this->tableName . " WHERE " . $condition;
				$command = $this->databaseConnection->prepare($sqlCommand);
				$command->execute();
			}
		}

		public function select($columns, $whereCommand) {
			$sqlCommand = "SELECT ";
			for ($count = 0; $count < count($columns); $count += 1) {
				if ($count > 0) {
					$sqlCommand = $sqlCommand . ", ";
				}
				$sqlCommand = $sqlCommand . $columns [$count];
			}
			$sqlCommand = $sqlCommand . " FROM " . $this->tableName;
			if ($whereCommand) {
				$sqlCommand = $sqlCommand . " WHERE " . $whereCommand;
			}
			$command = $this->databaseConnection->prepare($sqlCommand);
			$command->execute();
			return $command->fetchall();
		}

		public function selectAll($whereCommand) {
			$sqlCommand = "SELECT * FROM " . $this->tableName;
			if ($whereCommand) {
				$sqlCommand = $sqlCommand . " WHERE " . $whereCommand;
			}
			$command = $this->databaseConnection->prepare($sqlCommand);
			$command->execute();
			return $command->fetchall();
		}
	}

?>
