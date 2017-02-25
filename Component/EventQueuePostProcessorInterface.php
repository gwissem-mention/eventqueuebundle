<?php
namespace Celltrak\EventQueueBundle\Component;

/**
 * Interface for services called after the queued event is processed.
 *
 * @author Mike Turoff
 */
interface EventQueuePostProcessorInterface
{

    /**
     * Processes event information.
     * @param  integer $queueId
     * @param  string $eventName
     * @param  array $data
     * @param  string $pinKey
     * @param EventQueueProcessingResult $result
     * @return void
     */
    public function process(
        $queueId,
        $eventName,
        array $data,
        $pinKey,
        EventQueueProcessingResult $result
    );

}
