<?php

class PageCodification {
	private $codigos;
	private $paginasChaves;
	private $paginasExistentes;
	private $jaUsado;

	public function __construct($receivedArray) {
		$this->jaUsado = false;
		$this->paginasExistentes = $receivedArray;
		for ($position = 0; $position < count($this->paginasExistentes); $position += 1)
			$this->codigos [$position] = $this->nomeCodificado();
	}

	public function nomeCodificado() {
		$stringGerada = "";
		$tamanhoRetorno = 17;
		$capsLock = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
		$caracteres = "abcdefghijklmnopqrstuvwxyz";
		$numeros = "0123456789";
		$simbolos = "!@$^_=-."; /* /* */
		$semente = str_split($capsLock . $caracteres . $numeros . $simbolos);
		for ($position = 0; $position < $tamanhoRetorno; $position += 1) {
			$stringGerada = $stringGerada . $semente [rand(0, count($semente) - 1)];
		}
		return $stringGerada;
	}

	public function associaCodificacaoPagina() {
		if ($this->jaUsado == true)
			$this->destroyCodes();
		else
			$this->jaUsado = true;
		for ($position = 0; $position < count($this->paginasExistentes); $position += 1) {
			$chaves = $this->nomeCodificado();
			$this->codigos [$position] = $chaves;
			$this->paginasChaves [$this->codigos [$position]] = $this->paginasExistentes [$position];
		}
	}

	private function destroyCodes() {
		if (isset ($this->codigos)) {
			foreach ($this->codigos as $chaves) {
				unset ($this->paginasChaves [$chaves]);
			}
		}
	}

	private function verificaCodigoExiste($testaExiste) {
		foreach ($this->codigos as $chaves) {
			if ($testaExiste == $chaves) {
				return $chaves;
			}
		}
		return null/*$this->codigos [0]*/
			;
	}

	public function getCodigoPagina($chavePagina) {
		$chaveEncontrada = $this->verificaCodigoExiste($chavePagina);
		if ($chaveEncontrada)
			return ($this->paginasChaves != null) ? $this->paginasChaves [$chaveEncontrada] : null/*$this->paginasExistentes [0]*/
				;
		return null;
	}

	public function getCodigos($position) {
		if ($position < 0 || $position >= count($this->codigos))
			return $this->codigos [0];
		return $this->codigos [$position];
	}

	public function getChave($pagina) {
		for ($position = 0; $position < count($this->paginasExistentes); $position += 1) {
			if ($this->paginasChaves [$this->codigos [$position]] == $pagina)
				return $this->codigos [$position];
		}
		return $this->codigos [0];
	}
}

?>