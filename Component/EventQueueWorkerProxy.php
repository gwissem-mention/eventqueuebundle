<?php
namespace Celltrak\EventQueueBundle\Component;

use CTLib\Component\Monolog\Logger;

/**
 * Proxy for EventQueueWorker passed to listeners so they can make restricted
 * interactions with the worker.
 */
class EventQueueWorkerProxy
{

    /**
     * @var integer
     * Worker's unique identifier.
     */
    protected $workerId;

    /**
     * @var EventQueueChannel
     * Worker's assigned queue channel.
     */
    protected $channel;

    /**
     * @var Logger
     */
    protected $logger;


    /**
     * @param integer $workerId
     * @param EventQueueChannel $channel
     * @param Logger $logger
     */
    public function __construct(
        $workerId,
        EventQueueChannel $channel,
        Logger $logger
    ) {
        $this->workerId = $workerId;
        $this->channel  = $channel;
        $this->logger   = $logger;
    }

    /**
     * Checks worker in with event queue channel so monitor knows it's still
     * active.
     *
     * @return void
     */
    public function checkIn()
    {
        $this->channel->checkInWorker($this->workerId);
    }

    /**
     * {@inheritDoc}
     */
    public function __toString()
    {
        return "{EventQueueWorkerProxy-{$this->workerId}}";
    }


}
