<?php

namespace Keboola\Controller\Action;


class Exception extends  \Keboola\Exception
{
	const BAD_REQUEST = 400; // missing parameters, validation errors
	const UNAUTHORIZED = 401; // authentification required
	const FORBIDDEN = 403; // access denied - user don't have permissions for resource
	const NOT_FOUND = 404; // resource not found
	const METHOD_NOT_ALLOWED = 405;
	const INTERNAL_ERROR = 500; // application error - our fault should be fixed
}
