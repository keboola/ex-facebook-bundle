<?php

namespace Keboola\FacebookExtractorBundle\Controller;

use Keboola\FacebookExtractorBundle\Facebook\Api;
use Symfony\Bundle\FrameworkBundle\Controller\Controller;

class DefaultController extends Controller
{
    public function indexAction($name)
    {
        return $this->render('KeboolaFacebookExtractorBundle:Default:index.html.twig', array('name' => $name));
    }

    public function tokenAction()
    {
        $content = '';

        $callbackUrl = 'https://' . $this->getRequest()->getHttpHost() . $this->getRequest()->getPathInfo();
        $session = $this->getRequest()->getSession();

        if (!$this->getRequest()->get('code')) {
            $session->set("state", md5(uniqid()));
            $authorizationUrl = Api::authorizationUrl(
                $this->container->getParameter('facebook')['appId'],
                $callbackUrl,
                $session->get("state")
            );
            return $this->redirect($authorizationUrl);
        } elseif ($this->getRequest()->get('state') == $session->get('state')) {
            $fbApi = new Api();
            $userToken = $fbApi->userToken(
                $this->container->getParameter('facebook')['appId'],
                $this->container->getParameter('facebook')['appSecret'],
                urlencode($callbackUrl),
                $this->getRequest()->get('code')
            );
            $content = $this->_renderAlert('Your token: ' . $userToken);
        } else {
            $content = $this->_renderAlert('The security token has expired. Please reload the page and try again.', 'danger');
        }

        return $this->render('KeboolaFacebookExtractorBundle:Default:token.html.twig', array("content" => $content));
    }

    /**
     * @param $text
     * @param string $type
     * @return string
     */
    protected function _renderAlert($text, $type = 'success')
   	{
   		return sprintf('<div class="alert alert-%s" style="word-break: break-word">%s</div>', $type, htmlspecialchars($text));
   	}
}
