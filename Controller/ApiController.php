<?php

namespace Keboola\FacebookExtractorBundle\Controller;

use Keboola\StorageApi\Components;
use Symfony\Component\HttpFoundation\Request;

class ApiController extends \Syrup\ComponentBundle\Controller\ApiController
{
    /**
   	 * @param string $command
   	 * @param array $params
   	 * @return JobInterface
   	 */
   	protected function createJob($command, $params)
   	{
        $job = parent::createJob($command, $params);
        if (!$this->container->getParameter('facebook')) {
            $workerNr = 1;
        } else {
            $workerNr = rand(1 ,$this->container->getParameter('workers'));
        }

        $job->setLockName($job->getComponent() . '-' . $job->getProject()['id'] . '-' . $workerNr);
        return $job;

   	}

    public function deleteConfigAction(Request $request)
    {
        $this->storageApi->dropTable('sys.c-ex-facebook.' . $request->get("configId"));
	    return $this->createJsonResponse([
            'status' => 'ok'
	    ], 200);
    }

}
