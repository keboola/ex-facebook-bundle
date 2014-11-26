<?php
/**
 * Created by JetBrains PhpStorm.
 * User: Martin Halamíček
 * Date: 9.2.12
 * Time: 14:01
 * To change this template use File | Settings | File Templates.
 */

namespace Keboola\Log\Formatter;

class Json extends \Zend_Log_Formatter_Abstract
{

	/**
	 * Construct a Zend_Log driver
	 *
	 * @param  array|Zend_Config $config
	 * @return Zend_Log_FactoryInterface
	 */
	static public function factory($config)
	{
		return new self();
	}

	/**
	 * Formats data into a single line to be written by the writer.
	 *
	 * @param  array	$event	event data
	 * @return string			 formatted line to write to the log
	 */
	public function format($event)
	{
		unset($event['timestamp']);
		$event['priority'] = $event['priorityName'];
		unset($event['priorityName']);

		if (isset($event['duration']) && is_float($event['duration'])) {
			$event['duration'] = round($event['duration'], 4);
		}

		if (isset($event['exception']) && $event['exception'] instanceof \Exception) {
			$event['exception'] = (array) $event['exception'];
		}

		return json_encode($event);
	}

}