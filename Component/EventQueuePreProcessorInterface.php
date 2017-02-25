<?php
namespace Celltrak\EventQueueBundle\Component;

/**
 * Interface for services called before the queued event is processed.
 *
 * @author Mike Turoff
 */
interface EventQueuePreProcessorInterface
{

    /**
     * Processes event information.
     * @param  integer $queueId
     * @param  string $eventName
     * @param  array  &$data
     * @param  string $pinKey
     * @return void
     */
    public function process($queueId, $eventName, array &$data, $pinKey);


}
