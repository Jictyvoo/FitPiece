<?php

	namespace Fit_Piece\util;

	class StackNode {
		public $next;
		public $information;
	}

	class Stack {
		private $first;
		private $sizeVar = 0;

		public function __construct() {
			$this->first = null;
			$this->sizeVar = 0;
		}

		public function size() {
			return $this->sizeVar;
		}

		public function isEmpty() {
			return $this->first == null;
		}

		public function pop() {
			if ($this->first == null) return null;
			$temporary = $this->first->information;
			$this->first = $this->first->next;
			$this->sizeVar = $this->sizeVar - 1;
			return $temporary;
		}

		public function push($informationReceived) {
			$temporary = new StackNode ();
			$temporary->information = $informationReceived;
			$temporary->next = $this->first;
			$this->first = $temporary;
			$this->sizeVar = $this->sizeVar + 1;
		}

		public function peek() {
			if ($this->first == null) return null;
			return $this->first->information;
		}
	}

	?>