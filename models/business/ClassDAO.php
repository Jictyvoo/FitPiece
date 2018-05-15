<?php
class ClassDAO {
	private $columns;
	private $tableName;
	private $databaseConnection;
	public function __construct($tableDescription, $tableName, $databaseConnection) {
		$this->tableName = $tableName;
		$this->databaseConnection = $databaseConnection;
		$this->columns = $tableDescription;
	}
	private function commandCreation($sqlCommand, $count, $value) {
		if ($count > 0) {
			$sqlCommand = $sqlCommand . ", ";
		}
		$sqlCommand = $sqlCommand . $value;
		return $sqlCommand;
	}
	private function arrayToValueString($valuesArray) {
		$valuesCommand = "";
		if (count ( $valuesArray ) < (count ( $this->columns ))) {
			$count = - 1;
		} else {
			$count = 0;
		}
		foreach ( $this->columns as $key => $value ) {
			if ($count == - 1 || $count == count ( $valuesArray )) {
				$count += 1;
				continue;
			}
			if ($valuesArray [$count] == null && $value ["Null"] == "No") {
				return null;
			}
			$valuesCommand = $this->commandCreation ( $valuesCommand, $count, $valuesArray [$count] );
			$count += 1;
		}
		return $valuesCommand;
	}
	private function generateColumnFields($valuesArray) {
		$sqlCommand = "";
		if (count ( $valuesArray ) < (count ( $this->columns ))) {
			$count = - 1;
			$initialCount = - 1;
		} else {
			$count = 0;
			$initialCount = 0;
		}
		foreach ( $this->columns as $key => $value ) {
			if ($count == - 1 || $count == count ( $valuesArray )) {
				$count += 1;
				continue;
			}
			$sqlCommand = $this->commandCreation ( $sqlCommand, $count, $value ["Field"] );
			$count += 1;
		}
		return $sqlCommand;
	}
	private function isAssociative(array $testArray) {
		if (array () === $testArray)
			return false;
		return array_keys ( $testArray ) !== range ( 0, count ( $testArray ) - 1 );
	}
	private function insertAssociative($valuesArray) {
		$columnsString = "";
		$valuesString = "";
		foreach ( $valuesArray as $key => $value ) {
			if (strlen ( $columnsString ) > 0) {
				$columnsString = $columnsString . ", ";
				$valuesString = $valuesString . ", ";
			}
			if (is_numeric ( $key )) {
				require_once ("../util/exception/UnsupportedOperationException.php");
				throw new \Exception ( "Associative Array can only have string Keys" );
			}
			$columnsString = $columnsString . $key;
			$valuesString = $valuesString . $value;
		}
		$sqlCommand = "INSERT INTO " . $this->tableName . "( " . $columnsString . ") VALUES(" . $valuesString . ")";
		var_dump ( $sqlCommand );
	}
	private function insertCommon($valuesArray) {
		$sqlCommand = "INSERT INTO " . $this->tableName . "(";
		$sqlCommand = $sqlCommand . $this->generateColumnFields ( $valuesArray );
		$sqlCommand = $sqlCommand . ") VALUES(";
		$sqlCommand = $sqlCommand . $this->arrayToValueString ( $valuesArray ) . ")";
		var_dump ( $sqlCommand );
	}
	public function insert($valuesArray) {
		if ($this->isAssociative ( $valuesArray )) {
			$this->insertAssociative ( $valuesArray );
		} else {
			$this->insertCommon ( $valuesArray );
		}
	}
}
?>
