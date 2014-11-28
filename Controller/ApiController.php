<?php

namespace Keboola\FacebookExtractorBundle\Controller;

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


}
