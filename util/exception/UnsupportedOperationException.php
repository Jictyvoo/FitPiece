<?php

namespace Fit_Piece\Exception;

/**
 * Thrown to indicate that the requested operation is not supported.
 */
class UnsupportedOperationException extends \RuntimeException {
	public function __construct($message) {
		echo $message;
	}
}

?>