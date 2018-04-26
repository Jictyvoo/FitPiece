<?php
	class SystemConfiguration {
		private $infomations;

		public function __construct() {
			$jsonFile = file_get_contents ( '../config.json' );
			$this->informations = json_decode ( $jsonFile, true );
		}

		public function getDatabaseInformation() {
			return $this -> informations["database"];
		}
	}
?>