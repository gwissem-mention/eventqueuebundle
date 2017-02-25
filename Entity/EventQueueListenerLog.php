<?php
namespace Celltrak\EventQueueBundle\Entity;

use Doctrine\ORM\Mapping as ORM,
    CTLib\Entity\BaseEntity;

/**
 * EventQueueListenerLog.php
 *
 * @author Mike Turoff
 *
 * @ORM\Table(name="event_queue_listener_log")
 * @ORM\Entity(repositoryClass="CTLib\Repository\BaseRepository")
 */
class EventQueueListenerLog extends BaseEntity
{

    /**
     * @var integer $eventQueueListenerLogId
     *
     * @ORM\Column(name="event_queue_listener_log_id", type="integer", nullable=false)
     * @ORM\Id
     * @ORM\GeneratedValue(strategy="AUTO")
     */
    private $eventQueueListenerLogId;

    /**
     * @var integer $queueId
     *
     * @ORM\Column(name="queue_id", type="integer", nullable=false)
     */
    private $queueId;

    /**
     * @var string $listenerId
     *
     * @ORM\Column(name="listener_id", type="string", nullable=false, length=100)
     */
    private $listenerId;

    /**
     * @var integer $workerId
     *
     * @ORM\Column(name="worker_id", type="integer", nullable=false)
     */
    private $workerId;

    /**
     * @var boolean $isFinished
     *
     * @ORM\Column(name="is_finished", type="boolean", nullable=false)
     */
    private $isFinished;

    /**
     * @var integer $processingDuration
     *
     * @ORM\Column(name="processing_duration", type="integer", nullable=true)
     */
    private $processingDuration;

    /**
     * @var integer $memoryUsageDelta
     *
     * @ORM\Column(name="memory_usage_delta", type="integer", nullable=true)
     */
    private $memoryUsageDelta;

    /**
     * @var text $exception
     *
     * @ORM\Column(name="exception", type="text", nullable=true)
     */
    private $exception;

    /**
     * @var string $logThread
     *
     * @ORM\Column(name="log_thread", type="string", length=35, nullable=true)
     */
    private $logThread;

    /**
     * @var integer $addedOn
     *
     * @ORM\Column(name="added_on", type="integer", nullable=false)
     */
    protected $addedOn;


    /**
     * Set eventQueueListenerLogId
     *
     * @param integer $eventQueueListenerLogId
     */
    public function setEventQueueListenerLogId($eventQueueListenerLogId)
    {
        $this->eventQueueListenerLogId = $eventQueueListenerLogId;
    }

    /**
     * Get eventQueueListenerLogId
     *
     * @return integer $eventQueueListenerLogId
     */
    public function getEventQueueListenerLogId()
    {
        return $this->eventQueueListenerLogId;
    }

    /**
     * Set queueId
     *
     * @param integer $queueId
     */
    public function setQueueId($queueId)
    {
        $this->queueId = $queueId;
    }

    /**
     * Get queueId
     *
     * @return integer $queueId
     */
    public function getQueueId()
    {
        return $this->queueId;
    }

    /**
     * Set listenerId
     *
     * @param string $listenerId
     */
    public function setListenerId($listenerId)
    {
        $this->listenerId = $listenerId;
    }

    /**
     * Get listenerId
     *
     * @return string $listenerId
     */
    public function getListenerId()
    {
        return $this->listenerId;
    }

    /**
     * Set workerId
     *
     * @param integer $workerId
     */
    public function setWorkerId($workerId)
    {
        $this->workerId = $workerId;
    }

    /**
     * Get workerId
     *
     * @return integer $workerId
     */
    public function getWorkerId()
    {
        return $this->workerId;
    }

    /**
     * Set sequence
     *
     * @param integer $sequence
     */
    public function setSequence($sequence)
    {
        $this->sequence = $sequence;
    }

    /**
     * Get sequence
     *
     * @return integer $sequence
     */
    public function getSequence()
    {
        return $this->sequence;
    }

    /**
     * Set isFinished
     *
     * @param boolean $isFinished
     */
    public function setIsFinished($isFinished)
    {
        $this->isFinished = $isFinished;
    }

    /**
     * Get isFinished
     *
     * @return boolean $isFinished
     */
    public function getIsFinished()
    {
        return $this->isFinished;
    }

    /**
     * Set processingDuration
     *
     * @param integer $processingDuration
     */
    public function setProcessingDuration($processingDuration)
    {
        $this->processingDuration = $processingDuration;
    }

    /**
     * Get processingDuration
     *
     * @return integer $processingDuration
     */
    public function getProcessingDuration()
    {
        return $this->processingDuration;
    }

    /**
     * Set memoryUsageDelta
     *
     * @param integer $memoryUsageDelta
     */
    public function setMemoryUsageDelta($memoryUsageDelta)
    {
        $this->memoryUsageDelta = $memoryUsageDelta;
    }

    /**
     * Get memoryUsageDelta
     *
     * @return integer $memoryUsageDelta
     */
    public function getMemoryUsageDelta()
    {
        return $this->memoryUsageDelta;
    }

    /**
     * Set exception
     *
     * @param string $exception
     */
    public function setException($exception)
    {
        $this->exception = $exception;
    }

    /**
     * Get exception
     *
     * @return string $exception
     */
    public function getException()
    {
        return $this->exception;
    }

    /**
     * Set logThread
     *
     * @param string $logThread
     */
    public function setLogThread($logThread)
    {
        $this->logThread = $logThread;
    }

    /**
     * Get logThread
     *
     * @return string $logThread
     */
    public function getLogThread()
    {
        return $this->logThread;
    }

}
