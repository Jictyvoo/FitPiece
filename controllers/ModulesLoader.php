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
require_once ($amount . "models/SystemConfiguration.php");
require_once ($amount . "controllers/DatabaseController.php");
class ModulesLoader {
	private $systemConfiguration;
	private $databaseController;
	public function __construct() {
		$this->systemConfiguration = new SystemConfiguration ();
		$tempInformation_database = $this->systemConfiguration->getDatabaseInformation ();
		try {
			$this->databaseController = new DatabaseController ( $tempInformation_database ["database_user"], $tempInformation_database ["database_password"], $tempInformation_database ["database_host"], $tempInformation_database ["database_name"], $tempInformation_database ["main_SQL_script"], $tempInformation_database ["first_CSV_file"] );
		} catch ( PDOException $error ) {
			$this->databaseController = null;
		}
	}
	public function getDatabaseController() {
		return $this->databaseController;
	}
}

?>