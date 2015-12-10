<?php

namespace Keboola\FacebookExtractorBundle\Facebook;

use Syrup\ComponentBundle\Exception\ApplicationException;

class InvalidTokenException extends \Exception
{
    protected $account;
    protected $data;

    public function getAccount()
    {
        return $this->account;
    }

    public function setAccount($account)
    {
        $this->account = $account;
        return $this;
    }

    /**
     * @return mixed
     */
    public function getData()
    {
        return $this->data;
    }

    /**
     * @param mixed $data
     * @return $this
     */
    public function setData($data)
    {
        $this->data = $data;
        return $this;
    }
}

class ApiErrorException extends \Exception
{

}

class RequestErrorException extends \Exception
{

}


class CurlException extends \Exception
{

}

/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2012-12-14
 */
class Api
{
	/**
	 * Number of retries for one API call
	 */
	const RETRIES_COUNT = 10;

	/**
	 * Back off time before retrying API call
	 */
	const BACKOFF_INTERVAL = 15;

	/**
	 * Number of retries for API call due to token problems
	 */
	const TOKEN_ERRORS_RETRIES_COUNT = 3;

	/**
	 * Base url of Graph API
	 */
	const API_URL = 'https://graph.facebook.com/';

	/**
	 * Url for authorization
	 */
	const AUTH_URL = 'https://www.facebook.com/dialog/oauth?client_id=%s&scope=read_insights,email,manage_pages&redirect_uri=%s&state=%s';

	/**
	 * @var \Keboola\Log\Log
	 */
	private $_log;

	/**
	 * @var int
	 */
	private $_tokenErrorsCount;
	/**
	 * @var Zend_Http_Client
	 */
	private $_httpClient;

	public $runId;

	/**
	 * @param $log
	 */
	public function __construct()
	{
		$this->_httpClient = new \Zend_Http_Client(self::API_URL, array(
			'timeout' => 60,
			'user-agent' => "Keboola Connection FB Extractor"
		));
		$this->_httpClient->setMethod('GET');
	}

	/**
	 * Makes general call
	 * @param string $url Url path without boundary slashes
	 * @throws ApiErrorException
	 * @throws InvalidTokenException
	 * @throws RequestErrorException
	 * @throws Exception
	 * @return array
	 */
	public function call($url)
	{
		$e = null;
		$apiError = FALSE;
		$errorDescription = NULL;
		$backOffInterval = self::BACKOFF_INTERVAL;
		for ($i = 1; $i <= self::RETRIES_COUNT; $i++) {

			$apiError = FALSE;

			try {
				$this->_httpClient->setUri($url);
				$response = $this->_httpClient->request();
				$rawResult = $response->getBody();

				if ($rawResult) {

					$result = json_decode($rawResult, true);

					if ($result) {
						if (isset($result['error'])) {

							if (isset($result['error']['type'])
                                && $result['error']['type']=='OAuthException'
                                && !strstr($result['error']['message'], 'versions v2.0')) {
								if (
									strstr($result['error']['message'], 'Invalid OAuth access token') || // Wrong token
									strstr($result['error']['message'], 'Session has expired') || // Token expired
									strstr($result['error']['message'], 'The user must be an administrator') ||
									strstr($result['error']['message'], 'The session has been invalidated') || // User changed password
									strstr($result['error']['message'], 'Session does not match current stored session.') || // User probably changed password
									strstr($result['error']['message'], 'has not authorized application') || // User is not admin of the account
									strstr($result['error']['message'], 'was migrated to page ID') // Page migrated to other ID
								) {
									// Permanent token problem
									throw (new InvalidTokenException($result['error']['message']))->setData($result);
								} elseif (
									strstr($result['error']['message'], 'retry your request later') ||
									strstr($result['error']['message'], 'TSocket: Could not read') ||
									strstr($result['error']['message'], 'An unknown error has occurred')
								) {
									//"An unexpected error has occured. Please retry your request later."
									$apiError = TRUE;
								} elseif (
									strstr($result['error']['message'], 'request limit reached')
								) {
									// API limit reached (600 requests per 600 seconds per access token)
									$backOffInterval = 3 * 60;
								} elseif (
									strstr($result['error']['message'], 'Some of the aliases you requested do not exist') // Call to non-existing page
								) {
									throw new RequestErrorException($url . ': '. $rawResult);
								} else {

									// Might be a temporary problem
									$this->_tokenErrorsCount++;
									if ($this->_tokenErrorsCount >= self::TOKEN_ERRORS_RETRIES_COUNT) {
										throw (new InvalidTokenException('Unknown OAuth error: ' . $rawResult))->setData($result);
									}

								}
							} else {
                                $type = "";
                                if (isset($result['error']['type'])) {
                                    $type = $result['error']['type'];
                                }
                                if (isset($result['error']['message'])) {
                                    if ($type) {
                                        $message = $type . ": " . $result['error']['message'];
                                    } else {
                                        $message = $result['error']['message'];
                                    }
                                } else {
                                    $message = json_encode($result);
                                }
								throw new RequestErrorException($message);
							}
						} else if (isset($result['error_msg'])) {
							$apiError = TRUE;
						} else {
							// API call was ok
							$this->_tokenErrorsCount = 0;
							return $result;
						}
						$errorDescription = array('API error', $result);
					} else {
						$errorDescription = array('Json error', $rawResult);
						$apiError = true;
					}
				} else {
					$errorDescription = array('No output');
					$apiError = true;
				}
			} catch (\Zend_Http_Client_Exception $e) {
				$errorDescription = array('Zend_Http_Client_Exception error', array(
					'code' => $e->getCode(),
					'message' => $e->getMessage()
				));
			} catch (CurlException $e) {
				$errorDescription = array('CurlException error', array(
					'code' => $e->getCode(),
					'message' => $e->getMessage()
				));
			}

			$errorMessage = null;
			if (isset($result['error']['message'])) {
				$errorMessage = $result['error']['message'];
			} elseif (isset($result['error_msg'])) {
				$errorMessage = $result['error_msg'];
			} elseif ($e && $e instanceof Exception) {
				$errorMessage = $e->getMessage();
			}

			$sleep = $backOffInterval * ($i);
			sleep($sleep);
		}

		$errorDescription['url'] = $url;
		if ($apiError)
			throw new ApiErrorException(\Zend_Json::encode($errorDescription));
		else
			throw new ApplicationException(\Zend_Json::encode($errorDescription));
	}


	/**
	 * Makes recursive API call to get all paginated results
	 * @param string $url
	 * @return array
	 */
	public function paginatedCall($url)
	{
		$result = array();

		$continue = TRUE;
		do {

			$subResult = $this->call($url);
			if (isset($subResult['data']) && is_array($subResult['data']) && (count($subResult['data']) > 0)) {
				$result = array_merge($result, $subResult['data']);
			} else {
				$continue = FALSE;
			}

			if (!empty($subResult['paging']['next'])) {
				$url = $subResult['paging']['next'];
			} else {
				$continue = FALSE;
			}

		} while ($continue);

		return $result;
	}


	/**
	 * @param $url
	 * @throws \Exception
	 * @throws FacebookApi_InvalidTokenException
	 * @return string
	 */
	public function tokenCall($url)
	{

		$client = new \Zend_Http_Client($url);
		$client->setMethod('GET');

		$errorDescription = NULL;
		for ($i = 0; $i < self::RETRIES_COUNT; $i++) {
			try {
				$response = $client->request();
				$rawResult = $response->getBody();

				if ($rawResult) {
					if(substr($rawResult, 0, 13) == 'access_token=') {
						if (strstr($rawResult, '&')) {
							$tmpArr = explode('&', $rawResult);
							$rawResult = $tmpArr[0];
						}
						return substr($rawResult, 13);
					} else {
						try {
							$result = \Zend_Json::decode($rawResult);
							if(isset($result['error'])) {
								if (isset($result['error']['type']) && $result['error']['type']=='OAuthException') {
									if (
										strstr($result['error']['message'], 'Session has expired') || // Token expired
										strstr($result['error']['message'], 'The session has been invalidated') || // User changed password
										strstr($result['error']['message'], 'Session does not match current stored session.') || // User probably changed password
                                        strstr($result['error']['message'], 'This authorization code has expired.') // Auth code expired
									) {

										// Permanent token problem
										throw new InvalidTokenException($rawResult);

									}
								}
								$errorDescription = $result['error'];
							} else {
								$errorDescription = $rawResult;
							}
						} catch (\Zend_Http_Client_Exception $e) {
							$errorDescription = array('Zend_Http_Client_Exception error', array(
								'code' => $e->getCode(),
								'message' => $e->getMessage()
							));
						}
					}

				} else {
					$errorDescription = array('No output');
				}
			} catch (\Zend_Http_Client_Exception $e) {
				$errorDescription = array('Zend_Http_Client_Exception error', array(
					'code' => $e->getCode(),
					'message' => $e->getMessage()
				));
			}

			sleep(self::BACKOFF_INTERVAL * ($i + 1));
		}

		$errorDescription['url'] = $url;
		throw new ApplicationException('Keboola\Service\Facebook\Api::tokenCall() error: ' . \Zend_Json::encode($errorDescription));

	}

	/**
	 * @static
	 * @param $appId
	 * @param $callbackUrl
	 * @param $csrf
	 * @return string
	 */
	public static function authorizationUrl($appId, $callbackUrl, $csrf)
	{
		return sprintf(self::AUTH_URL, $appId, $callbackUrl, $csrf);
	}

    /**
     * @param $appId
     * @param $appSecret
     * @param $callback
     * @param $code
     * @return string
     * @throws InvalidTokenException
     * @throws \Exception
     */
	public function userToken($appId, $appSecret, $callback, $code)
	{
		$url = self::API_URL . "oauth/access_token?client_id=" . $appId . "&client_secret=" . $appSecret
			. "&redirect_uri=" . $callback . "&code=" . $code;
		return $this->tokenCall($url);
	}
}
