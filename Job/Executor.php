<?php

namespace Keboola\FacebookExtractorBundle\Job;

use Keboola\FacebookExtractorBundle\Facebook\Import;
use Keboola\FacebookExtractorBundle\Facebook\InvalidTokenException;
use Monolog\Logger;
use Monolog\Registry;
use Syrup\ComponentBundle\Exception\ApplicationException;
use Syrup\ComponentBundle\Exception\UserException;
use Syrup\ComponentBundle\Job\Executor as BaseExecutor;
use Syrup\ComponentBundle\Job\Metadata\Job;

class Executor extends BaseExecutor
{

    /**
     * @var Logger
     */
    private $log;

    /**
     * @var string
     */
    private $appName;

    public function __construct(Logger $log, $appName)
   	{
        $this->log = $log;
        $this->appName = $appName;
        Registry::addLogger($log, "log");
   	}


    /**
     * @param Job $job
     * @return array|void
     */
    public function execute(Job $job)
    {
        \Keboola\StorageApi\Config\Reader::$client = $this->storageApi;

        $configBucket = \Keboola\StorageApi\Config\Reader::read("sys.c-{$this->appName}", $this->storageApi->token);
        $passed = false;

        $accountsIds = array();
        $accountsOffset = 0;
        $accountsLimit = 0;

        $jsonParams = $job->getAttribute("params");

        if (isset($jsonParams["accountsIds"])) {
            $accountsIds = explode(',', $jsonParams["accountsIds"]);
        } else {
            if (isset($jsonParams["accountsOffset"]) && is_integer($jsonParams["accountsOffset"]) && $jsonParams["accountsOffset"] > 0) {
                $accountsOffset = $jsonParams["accountsOffset"];
            }
            if (isset($jsonParams["accountsLimit"]) && is_integer($jsonParams["accountsLimit"]) && $jsonParams["accountsLimit"] > 0) {
                $accountsLimit = $jsonParams["accountsLimit"];
            }
        }

        if (isset($jsonParams["since"])) {
            $since = $jsonParams["since"];
        } else {
            $days = isset($jsonParams["days"]) ? $jsonParams["days"] : 14;
            $since = '-' . $days . ' days';
        }

        $until = isset($jsonParams["until"]) ? $jsonParams["until"] : 'today';
        if (strtotime($since) > strtotime($until)) {
            return false;
        }

        if (isset($jsonParams["config"])) {
            $jsonParams["configurationId"] = $jsonParams["config"];
        }


        $reservedTables = array('accounts');
        foreach($configBucket["items"] as $configurationId => $configInstance) if (!in_array($configurationId, $reservedTables)) {

            if (count($jsonParams) && isset($jsonParams["configurationId"]) && $jsonParams["configurationId"] != $configurationId) {
                continue;
            }

            $passed = true;

            $connectionConfig = $configInstance;
            unset($connectionConfig["items"]);
            $connectionConfig = new \Zend_Config($connectionConfig, true);

            $runConfig = $configInstance["items"];
            $runConfig = new \Zend_Config($runConfig, true);

            try {

                $fbImport = new Import($this->appName);
                $fbImport->storageApi = $this->storageApi;
                $fbImport->runId = $this->storageApi->getRunId();

                $fbImport->log("Extraction of row {$configurationId} started");

                if (isset($configInstance["paging"])) {
                    $fbImport->paging = $configInstance["paging"];
                }

                $fbImport->configurationId = $configurationId;
                $fbImport->importConfig = $connectionConfig;
                $fbImport->runConfig = $runConfig;
                $fbImport->storageApiBucket = "in.c-{$this->appName}-" . $configurationId;

                \NDebugger::timer('configuration');
                $fbImport->log("Extraction of configuration $configurationId started", array(
                    'since' => $since,
                    'until' => $until,
                    'accountsOffset' => $accountsOffset,
                    'accountsLimit' => $accountsLimit,
                    'accountsIds' => $accountsIds
                ));

                if (!$this->storageApi->bucketExists($fbImport->storageApiBucket)) {
                    $this->storageApi->createBucket($this->appName . '-' . $configurationId, \Keboola\StorageApi\Client::STAGE_IN, "Facebook Extractor Data");
                }
                $tokenInfo = $this->storageApi->getLogData();

                $tmpDir = "/tmp/" . $tokenInfo["token"] . "-" . uniqid($configurationId . "-") . "/";

                if (!file_exists($tmpDir)) {
                    mkdir($tmpDir);
                }

                if (!is_dir($tmpDir)) {
                    throw new ApplicationException("Temporary directory path ($tmpDir) is not a directory", null, null, "TMP_DIR");
                }

                $fbImport->tmpDir = $tmpDir;

                $fbImport->import(
                    $since,
                    $until,
                    $accountsOffset,
                    $accountsLimit,
                    $accountsIds
                );

                $duration = \NDebugger::timer('configuration');
                $fbImport->log("Extraction of configuration $configurationId ended", array(), $duration);

                // Cleanup
                exec("rm -rf $tmpDir");
                $fbImport->log("Extraction of row {$configurationId} finished", array(), $duration);
            } catch (InvalidTokenException $e) {
                throw new UserException("Invalid account {$e->getAccount()} or token for this account: " . $e->getMessage(),
                    $e);
            } catch (UserException $e) {
                throw $e;
            } catch (\Exception $e) {
                throw new ApplicationException($e->getMessage(), $e);
            }
        }

        if (!$passed) {
            throw new UserException("ConfigurationId {$jsonParams["configurationId"]} not found");
        }

        $response = array("status" => "ok");
        return $response;
    }
}