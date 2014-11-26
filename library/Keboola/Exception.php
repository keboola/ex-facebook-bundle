<?php
/**
 *
 * User: Martin Halamíček
 * Date: 19.7.12
 * Time: 12:22
 *
 */

namespace Keboola;


class Exception extends \Exception
{

	protected $_stringCode;

	protected $_contextParams;

	public function __construct($message = NULL, $code = NULL, $previous = NULL, $stringCode = NULL, $params = NULL)
	{
		$this->setStringCode($stringCode);
		$this->setContextParams($params);
		parent::__construct($message, $code, $previous);
	}


	public function getStringCode()
	{
		return $this->_stringCode;
	}

	/**
	 * @param $stringCode
	 * @return Exception
	 */
	public function setStringCode($stringCode)
	{
		$this->_stringCode = (string) $stringCode;
		return $this;
	}

	public function getContextParams()
	{
		return $this->_contextParams;
	}

	/**
	 * @param array $contextParams
	 * @return Exception
	 */
	public function setContextParams($contextParams)
	{
		$this->_contextParams = (array) $contextParams;
		return $this;
	}
}