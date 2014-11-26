<?php

namespace Keboola\FacebookExtractorBundle\Job;

use Syrup\ComponentBundle\Job\Executor as BaseExecutor;
use Syrup\ComponentBundle\Job\Metadata\Job;

class Executor extends BaseExecutor
{
   public function execute(Job $job)
   {
	var_dump("test");
      return ["message" => "Hello Job " . $job->getId()];
   }

}