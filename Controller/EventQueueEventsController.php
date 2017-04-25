<?php
namespace Celltrak\EventQueueBundle\Controller;

use Celltrak\EventQueueBundle\Component\EventQueueDispatcher;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\HttpFoundation\Response;
use CTLib\Component\Monolog\Logger;


/**
 * Event Queue Events REST API.
 *
 *
 * @author Sean Hunter
 */
class EventQueueEventsController
{

    /**
     * @var EventQueueDispatcher
     */
    protected $eventQueueDispatcher;

    /**
     * @var Logger
     */
    protected $logger;


    /**
     * @param EventQueueDispatcher $eventQueueDispatcher
     * @param Logger $logger
     */
    public function __construct(
        EventQueueDispatcher $eventQueueDispatcher,
        Logger $logger
    ) {
        $this->eventQueueDispatcher = $eventQueueDispatcher;
        $this->logger               = $logger;
    }


    /**
     * Get content needed for dispatch and send via eventQueueDispatcher.
     * @param  Request $request
     * @return Response
     */
    public function dispatchAction(Request $request)
    {
        // get content for the dispatch call
        $body = $request->getContent();
        $data = json_decode($body, true);
        // check that we have data
        if ($data === null) {
            return new Response("EventQueueEventsController: missing required data for Dispatch", 400);
        } else {
            // check for required data items
            if (!isset($data['eventName'])) {
                return new Response("EventQueueEventsController: missing required data for eventName", 400);
            }

            if (isset($data['eventData']) && !is_array($data['eventData'])) {
                return new Response("EventQueueEventsController: eventData must be object", 400);
            }
        }

        $this->logger->debug("EventQueueEventsController: start dispatch for {$data['eventName']}");

        // fire eventQueue Dispatch event
        $queueEntryResponse = $this->eventQueueDispatcher->dispatch(
            $data['eventName'],
            isset($data['eventData']) ? $data['eventData'] : [],
            isset($data['referenceKey']) ? $data['referenceKey'] : null,
            isset($data['pinKey']) ? $data['pinKey'] : null
        );
        $responseBody = json_encode($queueEntryResponse);

        return new Response($responseBody, 200);
    }

}
