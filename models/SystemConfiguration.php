<?php
class SystemConfiguration {
	private $information;
	public function __construct() {
		$amount = "";
		$path = explode ( "/", $_SERVER ['SCRIPT_FILENAME'] );
		$rootPathArray = explode ( "/", $_SERVER ['DOCUMENT_ROOT'] );
		$rootPath = $rootPathArray [count ( $rootPathArray ) - 1];
		for($index = count ( $path ) - 2; $index >= 0; $index -= 1) {
			if ($path [$index] == $rootPath) {
				break;
			}
			$amount = $amount . "../";
		}
		$jsonFile = file_get_contents ( $amount . 'config.json' );
		$this->information = json_decode ( $jsonFile, true );
	}
	public function getDatabaseInformation() {
		return $this->information ["database"];
	}
}

?>