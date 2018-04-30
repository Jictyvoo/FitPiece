<?php

class ArrayList {
	private $mainVector;
	private $next;

	public function __construct() {
		$this->next = 0;
	}

	public function add($data) {
		$this->mainVector [$this->next] = $data;
		$this->next += 1;
	}

	public function set($index, $data) {
		$this->mainVector [$index] = $data;
	}

	public function get($index) {
		if ($index <= $this->next && $index >= 0)
			return $this->mainVector [$index];
		return null;
	}

	public function contains($obj) {
		for ($position = 0; $position < $this->next; $position = $position + 1) {
			if ($this->mainVector [$position] == $obj)
				return true;
		}
		return false;
	}

	public function removeIndex($index) {
		$this->mainVector [$index] = $this->mainVector [$this->next - 1];
		$this->next = $this->next - 1;
	}

	public function removeData($data) {
		for ($position = 0; $position < $this->next; $position = $position + 1) {
			if ($this->mainVector [$position] == $data) {
				$this->mainVector [$position] = $this->mainVector [$this->next - 1];
				$this->next = $this->next - 1;
				break;
			}
		}
	}

	public function size() {
		return $this->next;
	}

	public function isEmpty() {
		return $this->next == 0;
	}
}

?>