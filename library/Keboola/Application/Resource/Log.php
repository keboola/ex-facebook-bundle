<?php
/**
 * Json logger to syslog and file stream
 */

namespace Keboola\Application\Resource;


use Keboola;

class Log extends \Zend_Application_Resource_ResourceAbstract
{
	/**
	 * @var App_Log
	 */
	protected $_log;

	/**
	 * Defined by Zend_Application_Resource_Resource
	 *
	 * @return App_Log
	 */
	public function init()
	{
		$bootstrap = $this->getBootstrap();
		$config = $bootstrap->bootstrap('config')->getResource('config');

		$attachmentUploader = new Keboola\Log\DebugLogUploader($config->attachmentUploader);

		$log = new Keboola\Log\Log($attachmentUploader);
		$log->setEventItem('app', $config->app->name);
		$log->setEventItem('pid', getmypid());

		if (php_sapi_name() == 'cli') {
			if (!empty($_SERVER['argv'])) {
				$log->setEventItem('cliCommand', implode(' ', $_SERVER['argv']));
			}
		}

		$options = $this->getOptions();
		$syslogOptions = isset($options['syslog']) ? $options['syslog'] : array();
		$sysLogWriter = new \Zend_Log_Writer_Syslog($syslogOptions);
		$sysLogWriter->setFormatter(new Keboola\Log\Formatter\Json());
		$log->addWriter($sysLogWriter);

		\Zend_Registry::set('log', $log);
		return $log;
	}

}
