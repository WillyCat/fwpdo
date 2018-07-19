<?php
trait errorHandler
{
	var $handler;
	var $temporaryHandler;

	function
	setHandler($h): void
	{
		$this -> handler = $h;
	}

	function
	setTemporaryHandler($h): void
	{
		if ($h != '')
			$this -> temporaryHandler = $h;
	}

	function
	getHandler()
	{
		if ($this -> temporaryHandler != '')
			return $this -> temporaryHandler;
		return $this -> handler;
	}

	function
	errorHandler ($msg)
	{
		switch ($this -> getHandler())
		{
		case 'raise' :
		case 'exception' :
			throw new Exception ($msg);

		case 'return' :
			return $msg;

		case 'die' :
			die ($msg);
		}
	}

	function
	resetTemporaryHandler(): void
	{
		$this -> temporaryHandler = '';
	}
}

?>
