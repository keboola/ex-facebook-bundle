<?php

namespace Keboola\FacebookExtractorBundle\Controller;

use Symfony\Bundle\FrameworkBundle\Controller\Controller;

class DefaultController extends Controller
{
    public function indexAction($name)
    {
        return $this->render('KeboolaFacebookExtractorBundle:Default:index.html.twig', array('name' => $name));
    }
}
