<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2012-12-14
 *
 * Optimalizace
 * - necyklovat po accountech, ale po konfiguracnich radcich - nejdriv nacist posts vsech stranek, nahrat, a pak nacist insights vsech postu, nahrat!
 *
 */

namespace Keboola\FacebookExtractorBundle\Facebook;

use Keboola\StorageApi\Exception;
use Monolog\Registry;
use Syrup\ComponentBundle\Exception\UserException;

class Import
{
    /**
     *
     */
	const ACCOUNTS_TABLE_ID = 'accounts';

	/**
	 * @var Api
	 */
	private $_fbApi;
	/**
	 * @var \Keboola\StorageApi\Client
	 */
	public $storageApi;
	/**
	 * @var string
	 */
	public $storageApiBucket;
	/**
	 * @var string
	 */
	public $tmpDir;
	/**
	 * @var \Zend_Config;
	 */
	public $importConfig;
	/**
	 * @var \Zend_Config;
	 */
	public $runConfig;
	/**
	 * @var int;
	 */
	public $currentConfigRowNumber;

	public $runId;
	public $configurationId;
	public $defaultEndTime;
	public $defaultEndDate;
	public $defaultInsightsDate;
	public $paging = 25;
	private $_csvFiles = array();
	private $_sapiTableCache = array();
	private $componentName;
	private $bucket;

	public static $TIME_ZONE = 'America/Los_Angeles';

    /**
     *
     */
	public function __construct($componentName='ex-facebook')
	{
        $this->componentName = $componentName;
        $this->bucket = 'sys.c-' . $componentName;
		$this->_fbApi = new Api();
	}

    /**
     *
     */
	private function _populateSapiTableCache()
	{
		$bucket = $this->storageApi->getBucket($this->storageApiBucket);
		foreach($bucket["tables"] as $table) {
			$this->_sapiTableCache[$table["id"]] = $table;
		}
	}

	/**
	 * Run import
	 * @param string $since
	 * @param string $until
	 * @param int $accountsOffset
	 * @param int $accountsCount
	 * @param array $accountsIds
	 * @throws Exception
	 */
	public function import($since = '-14 days', $until = 'today', $accountsOffset = 0, $accountsCount = 0, $accountsIds = array())
	{
		$this->_fbApi->runId = $this->runId . '-' . $this->configurationId; //@TODO DEBUG
		$this->_populateSapiTableCache();

		$defaultDate = new \DateTime("now", new \DateTimeZone(self::$TIME_ZONE));
		$this->defaultEndTime = $defaultDate->format(\DateTime::ATOM);
		$this->defaultEndDate = $defaultDate->format('Y-m-d\T00:00:00P');
		$this->defaultInsightsDate = $defaultDate->sub(\DateInterval::createFromDateString("1 days"))->format('Y-m-d\T00:00:00P');

		$this->tmpDir = sprintf('%s/%s-%s', "/tmp/ex-fb", date('Ymd-His'), uniqid());
		mkdir($this->tmpDir, 0777, true);

		if (!$this->storageApi->tableExists($this->bucket . '.' . self::ACCOUNTS_TABLE_ID)) {
			$this->log('Accounts table does not exist in configuration', array(), 0, true);
			return;
		}

		$accountsCsv = $this->storageApi->exportTable($this->bucket . '.' . self::ACCOUNTS_TABLE_ID);
		$accounts = \Keboola\StorageApi\Client::parseCsv($accountsCsv);

		if (!count($accounts)) {
			$this->log('No accounts in configuration table', array(), 0, true);
		}

		// Create files for each configuration row
		$this->_prepareCsvFiles();

		// Iterate through each configuration row
		foreach ($this->runConfig as $queryNumber => $query) {
			$this->currentConfigRowNumber = $queryNumber+1;
			$accountsCounter = 0;
			foreach ($accounts as $account) {
				if (!isset($account["valid"]) || !$account['valid']) {
					continue 1;
				}

				// Run for the account only if specified or offset and count are matching
				if (!(
					(count($accountsIds) && in_array($account['id'], $accountsIds))
					|| (!count($accountsIds) && $accountsCounter >= $accountsOffset && ($accountsCount == 0 || $accountsCounter < $accountsOffset + $accountsCount))
					)) {
					$accountsCounter++;
					continue 1;
				}
				$accountsCounter++;

				if (!isset($account['id']) || !isset($account['token'])) {
					$this->log('Wrong configuration of accounts table', array(), 0, true);
					continue 1;
				}

				try {
					\NDebugger::timer('account');
					$this->_parseQuery($account, $query, $since, $until, $this->_csvFiles[$queryNumber]["handle"]);
				} catch (InvalidTokenException $e) {
					$this->_invalidateAccount($account['order'], $account['id']);
					$this->log('Invalid account \'' . $account['id'] . '\' or token', array('account' => $account['id'], 'error' => $e->getMessage()), 0, true);
				}
			}

			$this->_uploadCsvFile($queryNumber, $query->table);

			$this->log("Extraction of query {$queryNumber} finished", array(
				'queryNumber' => $query
			), \NDebugger::timer('account'), false);
		}
	}


	/*********
	 ********* Import
	 *********/

	/**
	 * @param $accountId
	 * @param $objectId
	 * @param $url
	 * @param $since
	 * @param $until
	 * @param $csvHandle
	 */
	private function _importInsightsPages($accountId, $objectId, $url, $since, $until, $csvHandle)
	{
		if ($until == 'today') {
			$until = 'yesterday';
		}

		// Iterate day by day
		$dateTime = new \DateTime($since, new \DateTimeZone(self::$TIME_ZONE));
		$untilDateTime = new \DateTime($until, new \DateTimeZone(self::$TIME_ZONE));

		$dateTime->setTime(0,0,0);
		$untilDateTime->setTime(0,0,0);
		$untilDateTime->add(\DateInterval::createFromDateString('1 day'));

		$interval = \DateInterval::createFromDateString('1 day');
		$days = new \DatePeriod($dateTime, $interval, $untilDateTime);
		foreach ($days as $day) {
			/** @var DateTime $day */
			$sinceTimestamp = strtotime($day->format(\DateTime::ATOM));
			$untilTimestamp = strtotime($day->add(\DateInterval::createFromDateString('1 day'))->format(\DateTime::ATOM));
			$callUrl = $url . '&since=' . $sinceTimestamp . '&until=' . $untilTimestamp;
			try {
				$data = $this->_fbApiCall($callUrl, "insights");
				$this->_parseInsightsData($accountId, $objectId, $data, false, $csvHandle);
			} catch (ApiErrorException $e) {
				$this->log('Graph API Error', array(
					'account' => $accountId,
					'url' => $callUrl,
					'error' => $e->getMessage()
				), 0, true);
			}
		}
	}


	/**
	 * @param $accountId
	 * @param $objectId
	 * @param $url
	 * @param $since
	 * @param $until
	 * @param $csvHandle
	 */
	private function _importInsightsPagesPivoted($accountId, $objectId, $url, $since, $until, $mapping, $csvHandle)
	{

		if ($until == 'today') {
			$until = 'yesterday';
		}

		// Iterate day by day

		$dateTime = new \DateTime($since, new \DateTimeZone(self::$TIME_ZONE));
		$untilDateTime = new \DateTime($until, new \DateTimeZone(self::$TIME_ZONE));

		// Reset to the whole day
		$dateTime->setTime(0,0,0);
		$untilDateTime->setTime(0,0,0);
		$untilDateTime->add(\DateInterval::createFromDateString('1 day'));

		$interval = \DateInterval::createFromDateString('1 day');
		$days = new \DatePeriod($dateTime, $interval, $untilDateTime);
		foreach ($days as $day) {
			/** @var DateTime $day */
			$endTime = $day->format(\DateTime::ATOM);
			$sinceTimestamp = strtotime($day->format(\DateTime::ATOM));
			$untilTimestamp = strtotime($day->add(\DateInterval::createFromDateString('1 day'))->format(\DateTime::ATOM));
			$callUrl = $url . '&since=' . $sinceTimestamp . '&until=' . $untilTimestamp;
			try {
				$data = $this->_fbApiCall($callUrl, "insights");
				$this->_extractInsightsData($accountId, $objectId, $data, $mapping, $endTime, $csvHandle);
			} catch (ApiErrorException $e) {
				$this->log('Graph API Error', array(
					'account' => $accountId,
					'url' => $callUrl,
					'error' => $e->getMessage()
				), 0, true);
			}
		}
	}

	/**
	 * @param $accountId
	 * @param $objectId
	 * @param $url
	 * @param $csvHandle
	 */
	private function _importInsightsPostsPivoted($accountId, $objectId, $url, $mapping, $csvHandle)
	{
		try {
			$data = $this->_fbApiCall($url, "insights");
			$day = new \DateTime("yesterday", new \DateTimeZone(self::$TIME_ZONE));
			$day->setTime(0,0,0);
			$endTime = $day->format(\DateTime::ATOM);
			$this->_extractInsightsData($accountId, $objectId, $data, $mapping, $endTime, $csvHandle);
		} catch (ApiErrorException $e) {
			$this->log('Graph API Error', array(
				'account' => $accountId,
				'url' => $url,
				'error' => $e->getMessage()
			), 0, true);
		}
	}

	/**
	 * @param $accountId
	 * @param $objectId
	 * @param $url
	 * @param $csvHandle
	 */
	private function _importInsightsPosts($accountId, $objectId, $url, $csvHandle)
	{
		try {
			$data = $this->_fbApiCall($url, "insights");
			$this->_parseInsightsData($accountId, $objectId, $data, true, $csvHandle);
		} catch (ApiErrorException $e) {
			$this->log('Graph API Error', array(
				'account' => $accountId,
				'url' => $url,
				'error' => $e->getMessage()
			), 0, true);
		}

	}

	/**
	 * @param $accountId
	 * @param $objectId
	 * @param $data
	 * @param $mapping
	 * @param $endTime
	 * @param $csvHandle
	 * @throws KeboolaException
	 * @throws Exception
	 */
	private function _extractInsightsData($accountId, $objectId, $data, $mapping, $endTime, $csvHandle)
	{
		$start = microtime(true);
		if (isset($data['data']) && is_array($data['data'])) {
			$columns = array();
			foreach ($mapping as $oneMapping) {
				if (!isset($oneMapping["column"]) || !isset($oneMapping["name"]) || !isset($oneMapping["period"])) {
					throw new UserException("Column mapping incorrect, mandatory values are column, name, period.");
				}
				$value = 0;
				foreach($data["data"] as $metric) {
					if ($metric["name"] == $oneMapping["name"] && $metric["period"] == $oneMapping["period"]) {
						if (isset($oneMapping["key"]) && isset($metric["values"][0]["value"][$oneMapping["key"]])) {
							$value = intval($metric["values"][0]["value"][$oneMapping["key"]]);
						} else {
							$value = intval($metric["values"][0]["value"]);
						}
					}
				}
				$columns[$oneMapping["column"]] = $value;
			}

			$writeStart = microtime(true);

			$output = array(
					md5($accountId . $objectId . $endTime),
					$accountId,
					$objectId,
					$endTime,
			);
			foreach($columns as $key => $value) {
				$output[$key] = $value;
			}

			fputcsv($csvHandle, $output, "\t", '"');

			fflush($csvHandle);
			if (microtime(true) - $writeStart > 0.1) {
			}
		} else {
			$this->log('Wrong response from Insights API', array(
				'account' => $accountId,
				'result' => $data
			), 0, true);
			throw new UserException(sprintf('Result for query on row %d is not valid.', $this->currentConfigRowNumber));
		}
	}


	/**
	 * @param $accountId
	 * @param $objectId
	 * @param $data
	 * @param bool $lifetime
	 * @param $csvHandle
	 * @throws Exception
	 */
	private function _parseInsightsData($accountId, $objectId, $data, $lifetime=false, $csvHandle)
	{
		$start = microtime(true);
		if (isset($data['data']) && is_array($data['data'])) {
			foreach ($data['data'] as $metricRow) {
				if (isset($metricRow['name']) && isset($metricRow['period']) && isset($metricRow['values'])
					&& is_array($metricRow['values'])) {
					foreach ($metricRow['values'] as $metricRowValue) {
						if (isset($metricRowValue['value'])) {
							if (is_array($metricRowValue['value'])) {

								// Iterate through metric drill
								foreach ($metricRowValue['value'] as $key => $value) {
									$endTime = isset($metricRowValue['end_time']) ? $metricRowValue['end_time'] : null;
									if (!$endTime && $metricRow['period'] == 'lifetime') {
										$endTime = $this->defaultInsightsDate;
									}
									$writeStart = microtime(true);

									// fix for metric post_impressions_histogram, where array is in value
									if (is_array($value)) {
										if (isset($value["value"])) {
											$value = $value["value"];
										} else {
											$value = 0;
										}
									}

									if ($lifetime) {
										fputcsv($csvHandle, array(
											md5($accountId . $objectId . $metricRow['name'] . $endTime . $key),
											$accountId,
											$objectId,
											$metricRow['name'],
											$endTime,
											$key,
											$value
										), "\t", '"');
									} else {
										fputcsv($csvHandle, array(
											md5($accountId . $objectId . $metricRow['name'] . $metricRow['period'] . $endTime . $key),
											$accountId,
											$objectId,
											$metricRow['name'],
											$metricRow['period'],
											$endTime,
											$key,
											$value
										), "\t", '"');
									}
									fflush($csvHandle);

								}
							} else {
								$endTime = isset($metricRowValue['end_time']) ? $metricRowValue['end_time'] : null;
								if (!$endTime && $metricRow['period'] == 'lifetime') {
									$endTime = $this->defaultInsightsDate;
								}
								$writeStart = microtime(true);
								if ($lifetime) {
									fputcsv($csvHandle, array(
										md5($accountId . $objectId . $metricRow['name'] . $endTime),
										$accountId,
										$objectId,
										$metricRow['name'],
										$endTime,
										'',
										$metricRowValue['value']
									), "\t", '"');
								} else {
									fputcsv($csvHandle, array(
										md5($accountId . $objectId . $metricRow['name'] . $metricRow['period'] . $endTime),
										$accountId,
										$objectId,
										$metricRow['name'],
										$metricRow['period'],
										$endTime,
										'',
										$metricRowValue['value']
									), "\t", '"');
								}
								fflush($csvHandle);

							}
						} else {
							$this->log('Wrong response from Insights API', array(
								'account' => $accountId,
								'result' => $data
							), 0, true);
							throw new UserException(sprintf('Result for query on row %d is not valid.', $this->currentConfigRowNumber));
						}
					}
				} else {
					$this->log('Wrong response from Insights API', array(
						'account' => $accountId,
						'result' => $data
					), 0, true);
					throw new UserException(sprintf('Result for query on row %d is not valid.', $this->currentConfigRowNumber));
				}
			}
		} else {
			$this->log('Wrong response from Insights API', array(
				'account' => $accountId,
				'result' => $data
			), 0, true);
			throw new UserException(sprintf('Result for query on row %d is not valid.', $this->currentConfigRowNumber));
		}
	}


	/**
	 * @param $accountId
	 * @param $objectId
	 * @param $url
	 * @param $since
	 * @param $until
	 * @param $compositePrimaryKey
	 * @param $timestampColumn
	 * @param $columnsToDownload
	 * @param $csvHandle
	 * @throws Exception
	 */
	private function _importList($accountId, $objectId, $url, $since, $until, $compositePrimaryKey, $timestampColumn, $columnsToDownload, $csvHandle)
	{
		if (!$timestampColumn) {
			$this->log(sprintf('Configuration query on row %d does not have timestamp column.', $this->currentConfigRowNumber), array(
				'account' => $accountId
			), 0, true);
			throw new UserException(sprintf('Configuration query on row %d does not have timestamp column.', $this->currentConfigRowNumber));
		}

		$dateTime = new \DateTime($since, new \DateTimeZone(self::$TIME_ZONE));
		$untilDateTime = new \DateTime($until, new \DateTimeZone(self::$TIME_ZONE));

		$dateTime->setTime(0,0,0);
		$untilDateTime->setTime(23,59,59);

		// Paginate data by defined number of results
		$paginate = true;
		$callUrl = $url . '&since=' . strtotime($dateTime->format(\DateTime::ATOM))
			. '&until=' . strtotime($untilDateTime->format(\DateTime::ATOM)) . '&limit=' . $this->paging;
		while($paginate) {
			try {
				$response = $this->_fbApiCall($callUrl, "list");

				if (isset($response['data']) && is_array($response['data'])) {
					$continue = false;

					foreach ($response['data'] as $dataRow) {
						// Check if we get data within wanted time frame
						$itemDate = date('Y-m-d', strtotime($dataRow[$timestampColumn]));
						if ($itemDate >= $dateTime->format('Y-m-d') && $itemDate <= $untilDateTime->format('Y-m-d')) {

							$csvRow = $this->_createObjectRow($dataRow, $columnsToDownload, $accountId, $objectId, $compositePrimaryKey);
							fputcsv($csvHandle, $csvRow, "\t", '"');
							$continue = true;
						}
					}
					if ($continue) {
						if (isset($response['paging']['next'])) {
							$callUrl = $response['paging']['next'];
						} else {
							$paginate = false;
						}
					} else {
						$paginate = false;
					}
				} else {
					$this->log('Wrong response from API', array(
						'account' => $accountId,
						'result' => $response
					), 0, true);
					throw new UserException(sprintf('Result for query on row %d is not valid.', $this->currentConfigRowNumber));
				}
			} catch (ApiErrorException $e) {
				$this->log('Graph API error', array(
					'account' => $accountId,
					'url' => $callUrl,
					'error' => $e->getMessage()
				), 0, true);
				$paginate = false;
			}
		}
	}


	/**
	 * @param $accountId
	 * @param $objectId
	 * @param $url
	 * @param $compositePrimaryKey
	 * @param $columnsToDownload
	 * @param $csvHandle
	 * @throws Exception
	 */
	private function _importPaginated($accountId, $objectId, $url, $compositePrimaryKey, $columnsToDownload, $csvHandle)
	{
		try {
			$response = $this->_fbApiPaginatedCall($url);
			if (is_array($response)) {
				foreach ($response as $dataRow) {
					$csvRow = $this->_createObjectRow($dataRow, $columnsToDownload, $accountId, $objectId, $compositePrimaryKey);
					fputcsv($csvHandle, $csvRow, "\t", '"');
				}
			} else {
				$this->log('Wrong response from API', array(
					'account' => $accountId,
					'result' => $response
				), 0, true);
				throw new UserException(sprintf('Result for query on row %d is not valid.', $this->currentConfigRowNumber));
			}
		} catch (ApiErrorException $e) {
			$this->log('Graph API error', array(
				'account' => $accountId,
				'url' => $url,
				'error' => $e->getMessage()
			), 0, true);
		}
	}


	/**
	 * @param $accountId
	 * @param $objectId
	 * @param $url
	 * @param $compositePrimaryKey
	 * @param $columnsToDownload
	 * @param $csvHandle
	 */
	private function _importObject($accountId, $objectId, $url, $compositePrimaryKey, $columnsToDownload, $csvHandle)
	{
		try {
			$response = $this->_fbApiCall($url, "object");

			if ($response) {
				$csvRow = $this->_createObjectRow($response, $columnsToDownload, $accountId, $objectId, $compositePrimaryKey);
				fputcsv($csvHandle, $csvRow, "\t", '"');

			} else {
				$this->log('No response from API', array(
					'account' => $accountId,
					'url' => $url
				), 0, true);
			}
		} catch (ApiErrorException $e) {
			$this->log('Graph API error', array(
				'account' => $accountId,
				'url' => $url,
				'error' => $e->getMessage()
			), 0, true);
		}
	}


	/**
	 * @param $accountId
	 * @param $objectId
	 * @param $url
	 * @param $columnsToDownload
	 * @param $csvHandle
	 */
	private function _importValue($accountId, $objectId, $url, $compositePrimaryKey, $columnsToDownload, $csvHandle)
	{
		try {
			$response = $this->_fbApiCall($url, "value");

			if ($response) {
				$csvRow = $this->_createObjectRow($response, $columnsToDownload, $accountId, $objectId, $compositePrimaryKey);
				fputcsv($csvHandle, $csvRow, "\t", '"');
			} else {
				$this->log('No response from API', array(
					'account' => $accountId,
					'url' => $url
				), 0, true);
			}
		} catch (ApiErrorException $e) {
			$this->log('Graph API error', array(
				'account' => $accountId,
				'url' => $url,
				'error' => $e->getMessage()
			), 0, true);
		}
	}

	/*********
	 ********* Processing and Saving
	 *********/


	private function _columnsNames($columnsToDownload)
	{
		$columnsNames = array_map(function($value) {
			return str_replace(array('#objectId', '.', '#'), array('ex__object', '_', ''), $value);
		}, $columnsToDownload);

		array_unshift($columnsNames, 'ex__account');
		array_unshift($columnsNames, 'ex__primary');

		return $columnsNames;
	}

	/**
	 * @param $response
	 * @param $columnsToDownload
	 * @param $accountId
	 * @param null $objectId
	 * @param bool|array $compositePrimaryKey
	 * @return array
	 */
	private function _createObjectRow($response, $columnsToDownload, $accountId, $objectId = null, $compositePrimaryKey = false)
	{
		$rowResult = array($accountId);
		$compositePrimaryKeyValue = $accountId;
		foreach ($columnsToDownload as $c) {
			$jsonResult = $this->jsonPath($response, $c);

			if (is_array($jsonResult)) {
				$rowValue = current($jsonResult);
			} elseif ($c == '#objectId') {
				$rowValue = $objectId;
			} elseif ($c == '#timestamp') {
				$rowValue = $this->defaultEndTime;
			} elseif ($c == '#datestamp') {
				$rowValue = $this->defaultEndDate;

			} else {
				$rowValue = null;
			}

			// Replace line breaks with spaces
			$rowValue = str_replace(array("\r\n", "\n"), ' ', $rowValue);

			if ($compositePrimaryKey && in_array($c, $compositePrimaryKey)) {
				$compositePrimaryKeyValue .= $rowValue;
			}

			$rowResult[] = $rowValue;

		}
		if ($compositePrimaryKey) {
			array_unshift($rowResult, $compositePrimaryKeyValue);
		}
		return $rowResult;
	}


    /**
     * @param $message
     * @param array $data
     * @param int $duration
     * @param bool $error
     * @throws \Keboola\StorageApi\Exception
     * @throws \Zend_Log_Exception
     */
	public function log($message, $data = array(), $duration = 0, $error = false)
	{
		$params = array_merge($data, array(
			'row' => $this->currentConfigRowNumber
		));

        $logger = Registry::getInstance("log");
        if ($error) {
            $logger->error($message, $data);
        } else {
            $logger->info($message, $data);
        }

		$event = new \Keboola\StorageApi\Event();
		$event->setComponent($this->componentName)
			->setConfigurationId($this->configurationId)
			->setRunId($this->runId)
			->setType($error ? 'error' : 'info')
			->setDuration($duration)
			->setMessage($message)
			->setParams($params);
		$this->storageApi->createEvent($event);
	}

	private function _invalidateAccount($order, $accountId)
	{
		$csvFile = $this->tmpDir . '/invalid-account-' . date('Ymd-His') . uniqid() . '.csv';

		$fp = fopen($csvFile, 'w');
		fputcsv($fp, array('order', 'id', 'valid'), "\t", '"');
		fputcsv($fp, array($order, $accountId, 0), "\t", '"');
		fclose($fp);

		$this->storageApi->writeTableAsync(
            $this->bucket . '.' . self::ACCOUNTS_TABLE_ID,
			new \Keboola\Csv\CsvFile($csvFile, "\t", '"', "\\"),
			array(
				"incremental" => true,
				"partial" => true
			)
		);

		unlink($csvFile);
	}

	/**
	 * @param $url
	 * @param string $type
	 * @return array
	 */
	private function _fbApiCall($url, $type="") {
		$start = microtime(true);
		$result = $this->_fbApi->call($url);
		return $result;
	}

	/**
	 * @param $url
	 * @param string $type
	 * @return array
	 */
	private function _fbApiPaginatedCall($url, $type="")
	{
		$start = microtime(true);
		$result = $this->_fbApi->paginatedCall($url);
		$duration = (microtime(true) - $start);
		return $result;
	}

	private function _parseQuery($account, $query, $since, $until, $csvHandle)
	{
		$parseQueryTime = microtime(true);

		// Fill configuration variables in query with values
		$parsedQuery = $query->query;

		if (strpos($parsedQuery, '[accountId]') !== false) {
			$parsedQuery = str_replace('[accountId]', $account['id'], $parsedQuery);
		}
		if (strpos($parsedQuery, '[') !== false) {
			$importConfig = $this->importConfig->toArray();
			if (is_array($importConfig)) foreach ($importConfig as $key => $value) {
				if (!is_array($value)) {
					$parsedQuery = str_replace('[' . $key . ']', $value, $parsedQuery);
				}
			}
		}

		$apiUrlParams = null;
		$columnsToDownload = array();
		if (in_array($query->type, array('insights', 'insightsLifetime', 'insightsPages', 'insightsPosts'))) {
			// Check if Insights type contains right url
			if (strpos($parsedQuery, '/insights') === FALSE) {
				throw new UserException(sprintf('Configuration query on row %d is not valid. Insights query needs
					to have format {objectId}/insights.', $this->currentConfigRowNumber));
			}
		} else {
			if (in_array($query->type, array('insights_pivoted', 'insightsLifetime_pivoted', 'insightsPages_pivoted', 'insightsPosts_pivoted'))) {
				try {
					$columnsToDownload = \Zend_Json::decode($query->columns);
				} catch(\Exception $e) {
					throw new UserException("Can't decode column mapping for insightsPages_pivoted or insightsPosts_pivoted.");
				}
			} elseif ($query->type != 'value') {
				// Find out which values to download
				$columnsToDownload = explode(',', $query->columns);
				if (count($columnsToDownload)) {
					$fields = array();
					foreach ($columnsToDownload as $column) {
						if (substr($column, 0, 1) != '#') {
							$fieldName = $column;
							$dotPosition = strpos($fieldName, '.');
							if ($dotPosition !== FALSE) {
								$fieldName = substr($fieldName, 0, $dotPosition);
							}
							if (!in_array($fieldName, $fields)) $fields[] = $fieldName;
						}
					}
					if (count($fields)) {
						$apiUrlParams = '&fields=' . urlencode(implode(',', $fields));
					}
				}
			} else {
				$columnsToDownload = explode(',', $query->columns);
			}
		}

		// Parse primary key column
		if (strpos($query->primaryColumn, '+') !== false) {
			$compositePrimaryKey = explode('+', $query->primaryColumn);
		} else {
			$compositePrimaryKey = array($query->primaryColumn);
		}

		// Default primary key for value type and add it also to columns to download
		if ($query->type == 'value') {
			if (in_array("#timestamp", $columnsToDownload)) {
				array_unshift($columnsToDownload, "#timestamp");
				array_unshift($compositePrimaryKey, "#timestamp");
			}
			if (in_array("#datestamp", $columnsToDownload)) {
				array_unshift($columnsToDownload, "#datestamp");
				array_unshift($compositePrimaryKey, "#datestamp");
			}
			array_unshift($columnsToDownload, "#objectId");
			array_unshift($compositePrimaryKey, "#objectId");
			$columnsToDownload = array_unique($columnsToDownload);
			$compositePrimaryKey = array_unique($compositePrimaryKey);
		}


		// If there is a table placeholder in query, run for each row from that table
		// So far we allow just one iteration
		// preg_match_all('/\{(\w+\.\w+)\}/U', $parsedQuery, $parsedResults);
		$matched = preg_match('/\{(\w+\.\w+(,\w+(:\d+)?)?)\}/U', $parsedQuery, $parsedResults);
		if ($matched) {

			// Iterate through given table's column rows and call API for each result from the table
			$placeholderConfig = $parsedResults[1];
			$tableConfig = explode('.', $placeholderConfig);
			$tableName = $tableConfig[0];
			$columnName = $tableConfig[1];

			$dataTableColumnsToFetch = array();

			// Check if there is a date limit
			$daysLimit = 0;
			$timestampColumn = '';
			$startTimestamp = 0;
			if (strpos($columnName, ',')) {
				$limit = explode(':',substr($columnName, strpos($columnName, ',')+1));
				if (!count($limit)==2) {
					throw new UserException(sprintf('Configuration query on row %d is not valid. Placeholder has wrong format.', $this->currentConfigRowNumber));
				}
				$timestampColumn = $limit[0];
				$daysLimit = isset($limit[1]) ? intval($limit[1]) : 0;
				$columnName = substr($columnName, 0, strpos($columnName, ','));
				$startTimestamp = $daysLimit > 0 ? strtotime('-' . $daysLimit . ' days') : null;
				$dataTableColumnsToFetch[] = $timestampColumn;
			}

			$dataTableColumnsToFetch[] = $columnName;

			$tableId = $this->storageApiBucket . '.' . $tableName;

			if (!isset($this->_sapiTableCache[$tableId])) {
				$this->log('Table \'$tableId\' for placeholders does not exist', array(), 0, true);
				return;
			}

			if(isset($this->_sapiTableCache[$tableId]["columns"])) {
				$tableConfig = $this->_sapiTableCache[$tableId];
			} else {
				$tableConfig = $this->storageApi->getTable($tableId);
				$this->_sapiTableCache[$tableId] = $tableConfig;
			}

			if (!in_array($columnName, $tableConfig['columns'])) {
				$this->log('Wrong configuration - wrong column in placeholder', array(), 0, true);
				return;
			}

			$duration = microtime(true) - $parseQueryTime;

			// Cache, download for all accounts, filter later
			$tmpFile = sprintf('%s/sapi-%s-%s-%d-%s.csv', $this->tmpDir, $tableName, $columnName, $daysLimit, $account['id']);
			if (!file_exists($tmpFile)) {
				$exportTimeStart = microtime(true);
				$exportParams = array(
					'columns' => $dataTableColumnsToFetch,
					'whereValues' => $account['id'],
					'whereColumn' => 'ex__account'
				);
				// Index column if needed and add to cache
				if (!in_array('ex__account', $this->_sapiTableCache[$tableId]["indexedColumns"])) {
					$tableInfo = $this->storageApi->getTable($tableId);
					if (!in_array('ex__account', $tableInfo["indexedColumns"])) {
						$this->storageApi->markTableColumnAsIndexed($tableId, 'ex__account');
						$this->_sapiTableCache[$tableId] = $this->storageApi->getTable($tableId);
					} else {
						$this->_sapiTableCache[$tableId] = $tableInfo;
					}
				}
				$this->storageApi->exportTable($tableId, $tmpFile, $exportParams);
			}
			$firstLine = true;
			$headers = array();
			$wantedRows = array();
			if (($handle = fopen($tmpFile, "r")) !== FALSE) {
				while ($tableRow = fgetcsv($handle, null, ",", '"', '"')) {
					if ($firstLine) {
						$headers = $tableRow;
					} else {
						$tableRow = array_combine($headers, $tableRow);
						// Do not call twice for the same row values (via $wantedRows array)
						if (in_array($tableRow[$columnName], $wantedRows)) {
							continue;
						}

						// Check days limit of data
						if (!(!$startTimestamp || strtotime($tableRow[$timestampColumn]) >= $startTimestamp)) {
							continue;
						}

						// Run query to API for each placeholder table row
						$url = str_replace('{' . $placeholderConfig . '}', $tableRow[$columnName], $parsedQuery);
						try {

							// Validate url
							$url = Api::API_URL . $url . (strpos($url, '?') === FALSE ? '?' : '&') . 'access_token=' . $account['token'] . $apiUrlParams;
                            $uri = \Zend_Uri_Http::fromString($url);
            				if (!$uri->valid()) {
								$this->log(sprintf('Configuration query on row %d is not valid.', $this->currentConfigRowNumber), array(
									'account' => $account['id'],
									'url' => $url
								), 0, true);
								throw new UserException(sprintf('Configuration query on row %d is not valid.', $this->currentConfigRowNumber));
							}
							$objectId = $tableRow[$columnName];

							switch ($query->type) {
								case 'insights':
								case 'insightsPages':
									$this->_importInsightspages($account['id'], $objectId, $url, $since, $until, $csvHandle);
									break;
								case 'insights_pivoted':
								case 'insightsPages_pivoted':
									$this->_importInsightsPagesPivoted($account['id'], $objectId, $url, $since, $until, $columnsToDownload, $csvHandle);
									break;
								case 'insightsLifetime_pivoted':
								case 'insightsPosts_pivoted':
									$this->_importInsightsPostsPivoted($account['id'], $objectId, $url, $columnsToDownload, $csvHandle);
									break;
								case 'insightsLifetime':
								case 'insightsPosts':
									$this->_importInsightsPosts($account['id'], $objectId, $url, $csvHandle);
									break;
								case 'list':
									$this->_importList($account['id'], $objectId, $url, $since, $until, $compositePrimaryKey, $timestampColumn, $columnsToDownload, $csvHandle);
									break;
								case 'paginated':
									$this->_importPaginated($account['id'], $objectId, $url, $compositePrimaryKey, $columnsToDownload, $csvHandle);
									break;
								case 'value':
									$this->_importValue($account['id'], $objectId, $url, $compositePrimaryKey, $columnsToDownload, $csvHandle);
									break;
								default:
									$this->_importObject($account['id'], $objectId, $url, $compositePrimaryKey, $columnsToDownload, $csvHandle);
							}

							$wantedRows[] = $tableRow[$columnName];

						} catch (RequestErrorException $e) {
							$this->log('Graph API Error', array(
								'account' => $account['id'],
								'url' => $url,
								'error' => $e->getMessage()
							), 0, true);
						}
					}
					$firstLine = false;
				}
				fclose($handle);
			}

		} else {

			$duration = microtime(true) - $parseQueryTime;

			// Run only for query in configuration row (there's no placeholder table)
			try {

				// Validate url
				$url = Api::API_URL . $parsedQuery . (strpos($parsedQuery, '?') === FALSE ? '?' : '&') . 'access_token=' . $account['token'] . $apiUrlParams;
                $uri = \Zend_Uri_Http::fromString($url);
				if (!$uri->valid()) {
					$this->log(sprintf('Configuration query on row %d is not valid.', $this->currentConfigRowNumber), array(
						'account' => $account['id'],
						'url' => $url
					), 0, true);
					throw new UserException(sprintf('Configuration query on row %d is not valid.', $this->currentConfigRowNumber));
				}
				$objectId = substr($parsedQuery, 0, strpos($parsedQuery, '/'));

				switch ($query->type) {
					case 'insights':
					case 'insightsPages':
						$this->_importInsightsPages($account['id'], $objectId, $url, $since, $until, $csvHandle);
						break;
					case 'insightsLifetime':
					case 'insightsPosts':
						$this->_importInsightsPosts($account['id'], $objectId, $url, $csvHandle);
						break;
					case 'insightsLifetime_pivoted':
					case 'insightsPosts_pivoted':
						$this->_importInsightsPostsPivoted($account['id'], $objectId, $url, $columnsToDownload, $csvHandle);
						break;
					case 'insights_pivoted':
					case 'insightsPages_pivoted':
						$this->_importInsightsPagesPivoted($account['id'], $objectId, $url, $since, $until, $columnsToDownload, $csvHandle);
						break;
					case 'list':
						$this->_importList($account['id'], $objectId, $url, $since, $until, $compositePrimaryKey, $query->timestampColumn, $columnsToDownload, $csvHandle);
						break;
					case 'paginated':
						$this->_importPaginated($account['id'], $objectId, $url, $compositePrimaryKey, $columnsToDownload, $csvHandle);
						break;
					case 'value':
						if (!$objectId && $query->query == "[accountId]") {
							$objectId = $account["id"];
						}
						$this->_importValue($account['id'], $objectId, $url, $compositePrimaryKey, $columnsToDownload, $csvHandle);
						break;
					default:
						$this->_importObject($account['id'], $objectId, $url, $compositePrimaryKey, $columnsToDownload, $csvHandle);
				}
			} catch (RequestErrorException $e) {
				$this->log('Graph API Error', array(
					'account' => $account['id'],
					'url' => $parsedQuery,
					'error' => $e->getMessage()
				), 0, true);
			}

		}
	}

	/**
	 *
	 * prepare and open CSV files
	 *
	 * @throws Exception
	 */
	private function _prepareCsvFiles()
	{
		foreach($this->runConfig as $queryNumber => $query) {

			// Fill configuration variables in query with values
			$parsedQuery = $query->query;

			if (strpos($parsedQuery, '[') !== false) {
				$importConfig = $this->importConfig->toArray();
				if (is_array($importConfig)) foreach ($importConfig as $key => $value) {
					if (!is_array($value)) {
						$parsedQuery = str_replace('[' . $key . ']', $value, $parsedQuery);
					}
				}
			}

			$columnsToDownload = array();
			if (in_array($query->type, array('insights', 'insightsPages', 'insightsLifetime', 'insightsPosts', 'insights_pivoted','insightsLifetime_pivoted', 'insightsPages_pivoted','insightsPosts_pivoted'))) {
				// Check if Insights type contains right url
				if (strpos($parsedQuery, '/insights') === FALSE) {
					throw new UserException(sprintf('Configuration query on row %d is not valid. Insights query needs
						to have format {objectId}/insights.', $queryNumber));
				}
				if (in_array($query->type, array('insights_pivoted', 'insightsLifetime_pivoted', 'insightsPages_pivoted','insightsPosts_pivoted'))) {
					try {
						$columnsToDownload = \Zend_Json::decode($query->columns);
					} catch(\Exception $e) {
						throw new UserException("Can't decode column mapping for insightsPages_pivoted or insightsPosts_pivoted.");

					}
				}
			} else {
				// Find out which values to download
				$columnsToDownload = explode(',', $query->columns);
			}

			// Create csv file for data
			$csvFileName = sprintf('%s/%d-%s.csv', $this->tmpDir, $queryNumber, uniqid());

			$csvHandle = fopen($csvFileName, 'w');
			// Add csv header
			if (in_array($query->type, array('insights', 'insightsPages'))) {
				fputcsv($csvHandle, array('ex__primary', 'ex__account', 'ex__object', 'metric', 'period', 'end_time', 'key', 'value'), "\t", '"');
			} elseif (in_array($query->type, array('insightsLifetime', 'insightsPosts'))) {
				fputcsv($csvHandle, array('ex__primary', 'ex__account', 'ex__object', 'metric', 'end_time', 'key', 'value'), "\t", '"');
			} elseif (in_array($query->type, array('insights_pivoted','insightsLifetime_pivoted', 'insightsPages_pivoted','insightsPosts_pivoted'))) {
				$columns = array('ex__primary', 'ex__account', 'ex__object', 'end_time');
				foreach($columnsToDownload as $column) {
					$columns[] = $column["column"];
				}
				fputcsv($csvHandle, $columns, "\t", '"');
			} else {
				if ($query->type == "value") {
					if (in_array("#timestamp", $columnsToDownload)) {
						array_unshift($columnsToDownload, "#timestamp");
					}
					if (in_array("#datestamp", $columnsToDownload)) {
						array_unshift($columnsToDownload, "#datestamp");
					}
					array_unshift($columnsToDownload, "#objectId");
					$columnsToDownload = array_unique($columnsToDownload);
				}
				$columnNames = $this->_columnsNames($columnsToDownload);
				fputcsv($csvHandle, $columnNames, "\t", '"');
			}
			$csvFile = array(
				"fileName" => $csvFileName,
				"handle" => $csvHandle
			);
			$this->_csvFiles[$queryNumber] = $csvFile;
		}
	}


	/**
     *
	 * Upload CSV file to storage API
     *
     * @param $queryNumber
     * @param $table
     * @return bool
     * @throws \Exception
     * @throws \Keboola\StorageApi\ClientException
     */
	private function _uploadCsvFile($queryNumber, $table)
	{
		if (!isset($this->_csvFiles[$queryNumber])) {
			return false;
		}
		$csvHandle = $this->_csvFiles[$queryNumber]["handle"];
		$csvFileName = $this->_csvFiles[$queryNumber]["fileName"];
		fclose($csvHandle);
		$start = microtime(true);
		exec("gzip \"$csvFileName\" --fast");
		$csvFileName .= ".gz";
		$start = microtime(true);
		if (file_exists($csvFileName)) {
			$tableId = $this->storageApiBucket . '.' . $table;
			if (!isset($this->_sapiTableCache[$tableId]) && $this->storageApi->tableExists($tableId)) {
				$this->_sapiTableCache[$tableId] = $this->storageApi->getTable($tableId);
			}
			try {
				if (isset($this->_sapiTableCache[$tableId])) {
					$this->storageApi->writeTableAsync(
						$tableId,
						new \Keboola\Csv\CsvFile($csvFileName, "\t", '"', "\\"),
						array(
							"incremental" => true
						));
				} else {
					$this->storageApi->createTableAsync(
						$this->storageApiBucket,
						$table,
						new \Keboola\Csv\CsvFile($csvFileName, "\t", '"', "\\"),
						array(
							"primaryKey" => "ex__primary"
						));
					$this->_sapiTableCache[$tableId] = $this->storageApi->getTable($tableId);
				}
			} catch (Exception $e) {
				throw new UserException($e->getMessage(), $e);
			}
		}
		return true;
	}

    /**
     *
     * JsonPath wrapper
     *
     * @param $obj
     * @param $expr
     * @param null $args
     * @return array|bool
     */
    public function jsonPath($obj, $expr, $args=null) {
       $jsonpath = new \JsonPath();
       $jsonpath->resultType = ($args ? $args['resultType'] : "VALUE");
       $x = $jsonpath->normalize($expr);
       $jsonpath->obj = $obj;
       if ($expr && $obj && ($jsonpath->resultType == "VALUE" || $jsonpath->resultType == "PATH")) {
          $jsonpath->trace(preg_replace("/^\\$;/", "", $x), $obj, "$");
          if (count($jsonpath->result))
             return $jsonpath->result;
          else
             return false;
       }
    }


}
