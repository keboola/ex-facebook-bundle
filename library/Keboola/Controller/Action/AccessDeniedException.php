<?php

namespace Keboola\Controller\Action;


class AccessDeniedException extends Exception
{

	public function __construct($message = 'You don\'t have access to resource.', $code = 403, $previous = NULL, $stringCode = NULL, $params = NULL)
	{
		parent::__construct($message, $code, $previous, $stringCode, $params = NULL);
	}

}