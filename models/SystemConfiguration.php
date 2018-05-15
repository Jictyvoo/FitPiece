<?php
class SystemConfiguration {
	private $information;
	public function __construct() {
		$jsonFile = file_get_contents ( '../config.json' );
		$this->information = json_decode ( $jsonFile, true );
	}
	public function getDatabaseInformation() {
		return $this->information ["database"];
	}
}

?>