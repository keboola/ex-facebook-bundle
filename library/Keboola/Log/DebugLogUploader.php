<?php
/**
 * S3 attachment uploader
 * User: Martin Halamíček
 * Date: 28.2.12
 * Time: 15:51
 *
 */

namespace Keboola\Log;

class DebugLogUploader
{

	/**
	 * @var array awsAccessKey, awsSecretKey, bitLyLogin, bitLyApiKey
	 */
	protected $_config;

	public function __construct($config)
	{
		$this->_config = $config;
	}

	public function upload($filePath, $contentType = 'text/plain')
	{

		$s3 = $this->_getS3();

		$s3FileName = $this->_fileUniquePrefix() . basename($filePath);
		$s3Path = $this->_config->s3path . '/' . $s3FileName;

		$s3->putFileStream($filePath, $s3Path, array(
			\Zend_Service_Amazon_S3::S3_CONTENT_TYPE_HEADER => $contentType,
			\Zend_Service_Amazon_S3::S3_ACL_HEADER => \Zend_Service_Amazon_S3::S3_ACL_PUBLIC_READ,
		));
		$url = 'https://s3.amazonaws.com/' . $s3Path;

		try {
			return $this->_shortenUrl($url);
		} catch (\Exception $e) {
			return $url;
		}
	}

	public function uploadString($name, $content, $contentType = 'text/plain')
	{
		$s3 = $this->_getS3();

		$s3FileName = $this->_fileUniquePrefix() . $name;
		$s3Path = $this->_config->s3path . '/' . $s3FileName;

		$s3->putObject($s3Path, $content, array(
			\Zend_Service_Amazon_S3::S3_CONTENT_TYPE_HEADER => $contentType,
			\Zend_Service_Amazon_S3::S3_ACL_HEADER => \Zend_Service_Amazon_S3::S3_ACL_PUBLIC_READ,
		));
		$url = 'https://s3.amazonaws.com/' . $s3Path;

		try {
			return $this->_shortenUrl($url);
		} catch (\Exception $e) {
			return $url;
		}
	}

	protected function _fileUniquePrefix()
	{
		return date('Y/m/') . date('Y-m-d-H-i-s') . '-' . uniqid() . '-';
	}

	protected function _shortenUrl($url)
	{
		$http = new \Zend_Http_Client();
		$http->setUri('http://api.bitly.com/v3/shorten');
		$http->setParameterGet('login', $this->_config->bitLyLogin);
		$http->setParameterGet('apiKey', $this->_config->bitLyApiKey);
		$http->setParameterGet('longUrl', $url);
		$http->setParameterGet('format', 'json');
		$res = $http->request('GET');
		$body = json_decode($res->getBody());

		if (!isset($body->data) || !isset($body->data->url)) {
			throw new \Exception('Bit.ly url not returned');
		}

		return $body->data->url;
	}

	/**
	 * @return \Zend_Service_Amazon_S3
	 */
	protected function _getS3()
	{
		return new \Zend_Service_Amazon_S3($this->_config->awsAccessKey, $this->_config->awsSecretKey);
	}

}