<?php
namespace Celltrak\EventQueueBundle\Component;

/**
 * Defines an event listener.
 *
 * @author Mike Turoff
 */
class EventQueueListener
{

    /**
     * Listener's unique ID.
     * @var string
     */
    protected $id;

    /**
     * Callback notified of event.
     * @var callable
     */
    protected $callback;

    /**
     * Set of listener IDs that must have completed before for this listener to
     * be called.
     * @var array
     */
    protected $requiredListenerIds;


    /**
     * @param string $id
     * @param callable $callback
     * @param array $requiredListenerIds
     */
    public function __construct(
        $id,
        callable $callback,
        array $requiredListenerIds = [])
    {
        $this->id = $id;
        $this->callback = $callback;
        $this->requiredListenerIds = $requiredListenerIds;
    }

    /**
     * Allows the listener to be a callable.
     * @param array $data   Event data.
     * @param EventQueueWorkerProxy $workerProxy
     * @return void
     */
    public function __invoke(array $data, EventQueueWorkerProxy $workerProxy)
    {
        call_user_func($this->callback, $data, $workerProxy);
        return;
    }

    /**
     * {@inheritDoc}
     */
    public function __toString()
    {
        return "{EventQueueListener-{$this->id}}";
    }

    /**
     * Returns $id.
     * @return string
     */
    public function getId()
    {
        return $this->id;
    }

    /**
     * Returns $requiredListenerIds.
     * @return array
     */
    public function getRequiredListenerIds()
    {
        return $this->requiredListenerIds;
    }
}
