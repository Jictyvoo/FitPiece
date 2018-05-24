<?php

	namespace Fit_Piece\models\value;

	class User {
		private $access_level;
		private $username;
		private $password;
		private $userId;

		public function __construct($access_level, $username, $password, $userId) {
			$this->access_level = $access_level;
			$this->username = $username;
			$this->password = $password;
			$this->userId = $userId;
		}

		public function getUsername() {
			return $this->username;
		}

		public function getAccessLevel() {
			return $this->access_level;
		}

		public function getUserID() {
			return $this->userId;
		}
	}

?>
