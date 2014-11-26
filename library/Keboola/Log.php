<?php
/**
 *
 * User: Martin Halamíček
 * Date: 28.2.12
 * Time: 14:55
 *
 */
namespace Keboola;

use Keboola\Log\DebugLogUploader;

class Log extends \Zend_Log
{

	/**
	 * @var Log\DebugLogUploader
	 */
	protected $_debugLogUploader;


	public function __construct(DebugLogUploader $debugLogUploader, \Zend_Log_Writer_Abstract $writer = null)
	{
		parent::__construct($writer);
		$this->setDebugLogUploader($debugLogUploader);
	}

	public function logWithAttachment($message, $priority, $attachment, $extras = NULL)
	{
		$extras = (array) $extras;
		$extras['attachment']  = $this->uploadAttachment($attachment);

		$this->log($message, $priority, $extras);
	}

	public function uploadAttachment($filePath, $contentType = 'text/plain')
	{
		try {
			return $this->_debugLogUploader->upload($filePath, $contentType);
		} catch (\Exception $e) {
			return  'Upload failed';
		}
	}

	public function getDebugLogUploader()
	{
		return $this->_debugLogUploader;
	}

	public function setDebugLogUploader(DebugLogUploader $attachmentUploader)
	{
		$this->_debugLogUploader = $attachmentUploader;
	}

}