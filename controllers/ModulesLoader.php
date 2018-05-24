<?php

	namespace Fit_Piece\controllers;
	use Fit_Piece\models\SystemConfiguration;

	class ModulesLoader {
		private $systemConfiguration;
		private $databaseController;

		public function __construct() {
			$this->systemConfiguration = new SystemConfiguration ();
			$tempInformation_database = $this->systemConfiguration->getDatabaseInformation();
			try {
				$this->databaseController = new DatabaseController ($tempInformation_database ["database_user"], $tempInformation_database ["database_password"], $tempInformation_database ["database_host"], $tempInformation_database ["database_name"], $tempInformation_database ["main_SQL_script"], $tempInformation_database ["first_CSV_file"]);
			} catch (PDOException $error) {
				$this->databaseController = null;
			}
		}

		public function getDatabaseController() {
			return $this->databaseController;
		}
	}

?>
