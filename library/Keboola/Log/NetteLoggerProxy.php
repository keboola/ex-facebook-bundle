<?php
/**
 * Logger for use with NDebugger - proxies to Zend_Log
 * User: Martin Halamíček
 * Date: 21.2.12
 * Time: 11:59
 *
 */

namespace Keboola\Log;

require_once 'Nette/NDebugger.php';

class NetteLoggerProxy extends \NLogger
{

	protected $_log;

	/**
	 * @param Log $log
	 */
	public function __construct(Log $log)
	{
		$this->_log = $log;
	}

	public function log($message, $priority = self::INFO)
	{
		$debugFile = FALSE;

		if (is_array($message)) {

			$firstPart = reset($message);

			// remove timestamp if present
			if (preg_match("/^\[[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}-[0-9]{2}-[0-9]{2}\]$/", $firstPart)) {
				array_shift($message);
			}

			$firstPart = reset($message);
			// find debug file - if not allowed memory exhausted
			if (strpos($firstPart, 'Fatal error: Allowed memory size of') !== 0) {
				foreach ($message as $i => $part) {
					$part = trim($part);
					if (strpos($part, '@@') === 0) {
						$debugFile = \NDebugger::$logDirectory . '/' . trim(str_replace('@@', '', $part));
						unset($message[$i]);
						break;
					}
				}
			}

			$message = implode(' ', $message);
		}

		if ($debugFile) {
			$attachment = $this->_log->uploadAttachment($debugFile, 'text/html');
			$this->_log->log($message, $this->_translatePriority($priority), array(
				'attachment' => $attachment,
			));
		} else {
			$this->_log->log($message, $this->_translatePriority($priority));
		}

	}

	protected function _translatePriority($priority)
	{
		static $translateMap = array(
			self::DEBUG => \Zend_Log::NOTICE,
			self::INFO => \Zend_Log::INFO,
			self::WARNING => \Zend_Log::WARN,
			self::ERROR => \Zend_Log::ERR,
			self::CRITICAL => \Zend_Log::ERR,
		);

		return isset($translateMap[$priority]) ? $translateMap[$priority] : \Zend_Log::ERR;
	}

}