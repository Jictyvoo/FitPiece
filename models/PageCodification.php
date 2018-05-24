<?php

	namespace Fit_Piece\models;

	class PageCodification {
		private $codes;
		private $pageKeys;
		private $existentPages;
		private $alreadyUsed;

		public function __construct($receivedArray) {
			$this->alreadyUsed = false;
			$this->existentPages = $receivedArray;
			for ($position = 0; $position < count($this->existentPages); $position += 1) $this->codes [$position] = $this->nomeCodificado();
		}

		public function nomeCodificado() {
			$generatedString = "";
			$returnSize = 17;
			$capsLock = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
			$characters = "abcdefghijklmnopqrstuvwxyz";
			$numbers = "0123456789";
			$symbols = "!@$^_=-."; /* /* */
			$seed = str_split($capsLock . $characters . $numbers . $symbols);
			for ($position = 0; $position < $returnSize; $position += 1) {
				$generatedString = $generatedString . $seed [rand(0, count($seed) - 1)];
			}
			return $generatedString;
		}

		public function associaCodificacaoPagina() {
			if ($this->alreadyUsed == true) $this->destroyCodes(); else
				$this->alreadyUsed = true;
			for ($position = 0; $position < count($this->existentPages); $position += 1) {
				$chaves = $this->nomeCodificado();
				$this->codes [$position] = $chaves;
				$this->pageKeys [$this->codes [$position]] = $this->existentPages [$position];
			}
		}

		private function destroyCodes() {
			if (isset ($this->codes)) {
				foreach ($this->codes as $key) {
					unset ($this->pageKeys [$key]);
				}
			}
		}

		private function verificaCodigoExiste($existenceTest) {
			foreach ($this->codes as $key) {
				if ($existenceTest == $key) {
					return $key;
				}
			}
			return null/*$this->codigos [0]*/
				;
		}

		public function getCodigoPagina($pageKey) {
			$foundKey = $this->verificaCodigoExiste($pageKey);
			if ($foundKey) return ($this->pageKeys != null) ? $this->pageKeys [$foundKey] : null/*$this->paginasExistentes [0]*/
				;
			return null;
		}

		public function getCodes($position) {
			if ($position < 0 || $position >= count($this->codes)) return $this->codes [0];
			return $this->codes [$position];
		}

		public function getChave($page) {
			for ($position = 0; $position < count($this->existentPages); $position += 1) {
				if ($this->pageKeys [$this->codes [$position]] == $page) return $this->codes [$position];
			}
			return $this->codes [0];
		}
	}

	?>