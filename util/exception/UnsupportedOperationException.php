<?php

namespace Odontoradiosis\Exception;

/**
 * Thrown to indicate that the requested operation is not supported.
 */
class UnsupportedOperationException extends \RuntimeException {
	public function __construct($message) {
		echo $message;
	}
}

?>