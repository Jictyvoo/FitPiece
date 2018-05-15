<?php
require_once "../models/DashboardsInformations.php";
class PageInformations {
	private $infomations;
	private $dashboardsInformations;
	public function __construct() {
		$jsonFile = file_get_contents ( '../layout/pagesInformations.json' );
		$this->informations = json_decode ( $jsonFile, true );
		$this->dashboardsInformations = new DashboardsInformations ( $this->informations ["accesLevels"] [0] );
	}
	public function getPageNames() {
		$temporary = $this->informations ["pages"] [0];
		$returnArray = array ();
		$count = 0;
		foreach ( $temporary as $key => $value ) {
			$returnArray [$count] = $key;
			$count += 1;
		}
		return $returnArray;
	}
	public function getInformationsAboutPage($pageName) {
		$temporary = $this->informations ["pages"] [0];
		if (isset ( $temporary [$pageName] )) {
			return $temporary [$pageName];
		}
		return $temporary ["404.php"];
	}
	public function getTitle($pageName) {
		$temporary = $this->getInformationsAboutPage ( $pageName );
		return $temporary ["pageName"];
	}
	public function getDefaultLayout($pageName) {
		$temporary = $this->getInformationsAboutPage ( $pageName );
		return $temporary ["defaultLayout"];
	}
	public function getFileLocation($pageName) {
		$temporary = $this->getInformationsAboutPage ( $pageName );
		return $temporary ["fileLocation"];
	}
	public function getUserAccessLevel($pageName) {
		$temporary = $this->getInformationsAboutPage ( $pageName );
		if (isset ( $temporary ["userAccessLevel"] )) {
			return $temporary ["userAccessLevel"];
		}
		return null;
	}
	public function getDashboardInformations() {
		return $this->dashboardsInformations;
	}
}

?>