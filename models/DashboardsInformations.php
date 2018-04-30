<?php

class DashboardsInformations {
	private $internalInformations;
	private $userLevelByDashboard;

	public function __construct($receivedArray) {
		$this->internalInformations = $receivedArray;
		foreach ($receivedArray as $key => $value) {
			$this->userLevelByDashboard [$value ['dashboard']] = $key;
		}
	}

	private function searchInformation($idReceived) {
		$searchId = 0;
		if (is_numeric($idReceived)) {
			if (!isset ($this->internalInformations [$idReceived])) {
				return null;
			}
			$searchId = $idReceived;
		} else {
			if (isset ($this->userLevelByDashboard [$idReceived])) {
				$searchId = $this->userLevelByDashboard [$idReceived];
			} else {
				return null;
			}
		}
		return $searchId;
	}

	public function getDashboard($idReceived) {
		$foundedInformation = $this->searchInformation($idReceived);
		return $foundedInformation != null ? $this->internalInformations [$foundedInformation] ['dashboard'] : null;
	}

	public function getMainPage($idReceived) {
		$foundedInformation = $this->searchInformation($idReceived);
		return $foundedInformation != null ? $this->internalInformations [$foundedInformation] ['mainPage'] : null;
	}

	public function getRedirectPages($idReceived) {
		$foundedInformation = $this->searchInformation($idReceived);
		return $foundedInformation != null ? $this->internalInformations [$foundedInformation] ['redirectPages'] : null;
	}
}

?>