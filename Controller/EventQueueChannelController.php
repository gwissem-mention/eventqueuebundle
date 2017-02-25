<?php
namespace CellTrak\EventQueueBundle\Component\Controller;

use CTLib\Component\Monolog\Logger;
use Symfony\Component\HttpFoundation\JsonResponse;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\HttpFoundation\Response;

/**
 * Event Queue Channel REST API.
 */
class EventQueueChannelController
{


    /**
     * @var Logger $logger
     */
    protected $logger;


    /**
     * @param EntityManager $entityManager
     * @param Logger $logger
     */
    public function __construct(Logger $logger) {
        $this->logger = $logger;
    }


}
