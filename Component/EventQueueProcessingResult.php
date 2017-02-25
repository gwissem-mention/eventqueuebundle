<?php
namespace Celltrak\EventQueueBundle\Component;

/**
 * Captures the result of processing an event and its listeners.
 *
 * @author Mike Turoff
 */
class EventQueueProcessingResult
{

    /**
     * Queue ID for this processing result.
     * @var integer
     */
    protected $queueId;

    /**
     * Set of listener IDs successfully finished for this event.
     * @var array
     */
    protected $finishedListenerIds;

    /**
     * Time at which the processing started for this event.
     * @var integer
     */
    protected $startTime;

    /**
     * Logger thread active during event processing.
     * @var string
     */
    protected $logThread;

    /**
     * Exception string logged at the event level (note: listeners log their
     * specific exception within $listenerResults).
     * @var string
     */
    protected $exception;

    /**
     * Set of listener IDs for this event that encountered an exception.
     * @var array
     */
    protected $failedListenerIds;

    /**
     * Total number of seconds spent processing this event.
     * @var integer
     */
    protected $totalProcessingDuration;

    /**
     * Indicates whether processing of this event was blocked by another event
     * asssigned to the same pin key.
     * @var boolean
     */
    protected $isBlocked;

    /**
     * Indicates that loggging has stopped.
     * @var boolean
     */
    protected $isStopped;


    /**
     * @param integer $queueId
     * @param array $alreadyFinishedListenerIds The listener IDs already
     *                                          successfully finished for this
     *                                          event.
     */
    public function __construct($queueId, array $alreadyFinishedListenerIds) {
        $this->queueId                  = $queueId;
        $this->finishedListenerIds      = $alreadyFinishedListenerIds;
        $this->startTime                = null;
        $this->logThread                = null;
        $this->exception                = null;
        $this->failedListenerIds        = [];
        $this->totalProcessingDuration  = 0;
        $this->isBlocked                = false;
        $this->isStopped                = false;
    }

    /**
     * Starts logging.
     * @param string $logThread
     * @return void
     */
    public function startLogging($logThread)
    {
        if (!is_null($this->startTime)) {
            throw new \RuntimeException("{$this} is already started");
        }

        $this->startTime = time();
        $this->logThread = $logThread;
    }

    /**
     * Stops logging.
     * @param string $exception
     * @return void
     */
    public function stopLogging($exception = null)
    {
        if (is_null($this->startTime)) {
            throw new \RuntimeException("{$this} is not started");
        }

        if ($this->isStopped) {
            throw new \RuntimeException("{$this} is already stopped");
        }

        $this->exception = $exception;
        $this->isStopped = true;
    }

    /**
     * Stops logging due to event being blocked by earlier pinned event.
     * @return void
     */
    public function blocked()
    {
        if (is_null($this->startTime)) {
            throw new \RuntimeException("{$this} is not started");
        }

        if ($this->isStopped) {
            throw new \RuntimeException("{$this} is already stopped");
        }

        $this->isBlocked = true;
        $this->isStopped = true;
    }

    /**
     * Records listener processing result.
     * @param EventQueueListenerProcessingResult $listenerResult
     * @return void
     */
    public function recordListenerResult(
        EventQueueListenerProcessingResult $listenerResult
    ) {
        if (is_null($this->startTime)) {
            throw new \RuntimeException("{$this} is not started");
        }

        if ($this->isStopped) {
            throw new \RuntimeException("{$this} is already stopped");
        }

        if ($listenerResult->isStopped() == false) {
            throw new \RuntimeException("{$listenerResult} is not stopped");
        }

        if ($listenerResult->hasException()) {
            $this->failedListenerIds[] = $listenerResult->getListenerId();
        } else {
            $this->finishedListenerIds[] = $listenerResult->getListenerId();
        }

        $this->totalProcessingDuration += $listenerResult->getProcessingDuration();
    }

    /**
     * Returns queueId.
     * @return integer
     */
    public function getQueueId()
    {
        return $this->queueId;
    }

    /**
     * Returns $startTime.
     * @return integer
     */
    public function getStartTime()
    {
        return $this->startTime;
    }

    /**
     * Returns $totalProcessingDuration.
     * @return integer
     */
    public function getTotalProcessingDuration()
    {
        return $this->totalProcessingDuration;
    }

    /**
     * Indicates whether event had any listeners processed.
     * @return boolean
     */
    public function hasProcessedListeners()
    {
        return $this->finishedListenerIds || $this->failedListenerIds;
    }

    /**
     * Returns $finishedListenerIds.
     * @return array
     */
    public function getFinishedListenerIds()
    {
        return $this->finishedListenerIds;
    }

    /**
     * Indicates whether any listener finished successfully.
     * @return boolean
     */
    public function hasFinishedListenerIds()
    {
        return $this->finishedListenerIds ? true : false;
    }

    /**
     * Indicates whether specified listener finished successfully.
     * @param string $listenerId
     * @return boolean
     */
    public function isFinishedListenerId($listenerId)
    {
        return in_array($listenerId, $this->finishedListenerIds);
    }

    /**
     * Returns $failedListenerIds.
     * @return array
     */
    public function getFailedListenerIds()
    {
        return $this->failedListenerIds;
    }

    /**
     * Indicates whether any listener encountered an exception.
     * @return boolean
     */
    public function hasFailedListenerIds()
    {
        return $this->failedListenerIds ? true : false;
    }

    /**
     * Indicates whether specified listener encountered an exception.
     * @param string $listenerId
     * @return boolean
     */
    public function isFailedListenerId($listenerId)
    {
        return in_array($listenerId, $this->failedListenerIds);
    }

    /**
     * Indicates whether processing was blocked.
     * @return boolean
     */
    public function isBlocked()
    {
        return $this->isBlocked;
    }

    /**
     * Returns $exception.
     * @return string
     */
    public function getException()
    {
        return $this->exception;
    }

    /**
     * Indicates whether processing was successful meaning:
     *      (1) No failed listeners
     *      (2) Processing was not blocked
     *      (3) There was no event-level exception
     * @return boolean
     */
    public function isSuccessful()
    {
        return
            !$this->failedListenerIds
            && !$this->isBlocked
            && !$this->exception;
    }

    /**
     * Returns $logThread.
     * @return string
     */
    public function getLogThread()
    {
        return $this->logThread;
    }

    /**
     * Indicates whether logging has been stopped.
     * @return boolean
     */
    public function isStopped()
    {
        return $this->isStopped;
    }

    /**
     * {@inheritDoc}
     */
    public function __toString()
    {
        return "{EventQueueProcessingResult-{$this->queueId}}";
    }
}
