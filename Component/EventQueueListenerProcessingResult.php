<?php
namespace Celltrak\EventQueueBundle\Component;

/**
 * Captures the result of processing an event's listener.
 * @author Mike Turoff
 */
class EventQueueListenerProcessingResult
{

    /**
     * Listener ID for this processing result.
     * @var string
     */
    protected $listenerId;

    /**
     * Time at which the processing started for this listener.
     * @var integer
     */
    protected $startTime;

    /**
     * Logger thread active during listener processing.
     * @var string
     */
    protected $logThread;

    /**
     * Script's memory usage when the processing started for this listener.
     * @var integer
     */
    protected $startMemoryUsage;

    /**
     * Exception string logged when processing this listener.
     * @var string
     */
    protected $exception;

    /**
     * Total number of seconds elapsed processing this listener.
     * @var integer
     */
    protected $processingDuration;

    /**
     * Total number of bytes used processing this listener.
     * @var integer
     */
    protected $memoryUsageDelta;

    /**
     * Indicates that loggging has stopped.
     * @var boolean
     */
    protected $isStopped;


    /**
     * @param string $listenerId
     */
    public function __construct($listenerId)
    {
        $this->listenerId           = $listenerId;
        $this->startTime            = null;
        $this->logThread            = null;
        $this->startMemoryUsage     = null;
        $this->exception            = null;
        $this->processingDuration   = 0;
        $this->memoryUsageDelta     = 0;
        $this->isStopped            = false;
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
        $this->startMemoryUsage = memory_get_usage();
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
        $this->processingDuration = time() - $this->startTime;
        $this->memoryUsageDelta = memory_get_usage() - $this->startMemoryUsage;
        $this->isStopped = true;
    }

    /**
     * Returns listenerId.
     * @return string
     */
    public function getListenerId()
    {
        return $this->listenerId;
    }

    /**
     * Returns logThread.
     * @return string
     */
    public function getLogThread()
    {
        return $this->logThread;
    }

    /**
     * Returns exception.
     * @return string
     */
    public function getException()
    {
        return $this->exception;
    }

    /**
     * Indicates whether exception logged.
     * @return boolean
     */
    public function hasException()
    {
        return !is_null($this->exception);
    }

    /**
     * Returns processingDuration.
     * @return integer
     */
    public function getProcessingDuration()
    {
        return $this->processingDuration;
    }

    /**
     * Returns memoryUsageDelta.
     * @return integer
     */
    public function getMemoryUsageDelta()
    {
        return $this->memoryUsageDelta;
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
        return "{EventQueueListenerProcessingResult-{$this->listenerId}}";
    }


}
