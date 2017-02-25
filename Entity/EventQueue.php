<?php
namespace Celltrak\EventQueueBundle\Entity;

use Doctrine\ORM\Mapping as ORM;
use CTLib\Entity\BaseEntity;

/**
 * EventQueue.php
 *
 * @author Mike Turoff
 *
 * @ORM\Table(name="event_queue")
 * @ORM\Entity(repositoryClass="Celltrak\EventQueueBundle\Repository\EventQueueRepository")
 * @ORM\HasLifecycleCallbacks
 */
class EventQueue extends BaseEntity implements \JsonSerializable
{

    /**
     * Statuses.
     */
    const STATUS_PENDING            = 'P';
    const STATUS_FINISHED           = 'F';
    const STATUS_EXCEPTION_OCCURRED = 'E';
    const STATUS_SKIPPED            = 'S';
    const STATUS_NOT_DISPATCHED     = 'N';
    const STATUS_NO_CHANNEL         = 'C';
    const STATUS_NO_LISTENERS       = 'L';



    /**
     * @var string $queueId
     *
     * @ORM\Column(name="queue_id", type="string", nullable=false, length=40)
     * @ORM\Id
     * @ORM\GeneratedValue(strategy="NONE")
     */
    private $queueId;

    /**
     * @var string $event
     *
     * @ORM\Column(name="event", type="string", length=35, nullable=false)
     */
    private $event;

    /**
     * @var text $data
     *
     * @ORM\Column(name="data", type="text", nullable=true)
     */
    private $data;

    /**
     * @var string $referenceKey
     *
     * @ORM\Column(name="reference_key", type="string", length=20, nullable=true)
     */
    private $referenceKey;

    /**
     * @var string $pinKey
     *
     * @ORM\Column(name="pin_key", type="string", length=20, nullable=true)
     */
    private $pinKey;

    /**
     * @var string $status
     *
     * @ORM\Column(name="status", type="string", length=1, nullable=false)
     */
    private $status;

    /**
     * @var string $channel
     *
     * @ORM\Column(name="channel", type="string", length=20, nullable=true)
     */
    private $channel;

    /**
     * @var integer $workerId
     *
     * @ORM\Column(name="worker_id", type="integer", nullable=true)
     */
    private $workerId;

    /**
     * @var integer $processingWait
     *
     * @ORM\Column(name="processing_wait", type="integer", nullable=true)
     */
    private $processingWait;

    /**
     * @var integer $processingDuration
     *
     * @ORM\Column(name="processing_duration", type="integer", nullable=true)
     */
    private $processingDuration;

    /**
     * @var text $finishedListenerIds
     */
    private $finishedListenerIds;

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
     * @var integer $modifiedOn
     *
     * @ORM\Column(name="modified_on", type="integer", nullable=false)
     */
    protected $modifiedOn;

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
     * Set event
     *
     * @param string $event
     */
    public function setEvent($event)
    {
        $this->event = $event;
    }

    /**
     * Get event
     *
     * @return string $event
     */
    public function getEvent()
    {
        return $this->event;
    }

    /**
     * Set data
     *
     * @param string $data
     */
    public function setData($data)
    {
        $this->data = $data;
    }

    /**
     * Get data
     *
     * @return string $data
     */
    public function getData()
    {
        return $this->data;
    }

    /**
     * Set referenceKey
     *
     * @param string $referenceKey
     */
    public function setReferenceKey($referenceKey)
    {
        $this->referenceKey = $referenceKey;
    }

    /**
     * Get referenceKey
     *
     * @return string $referenceKey
     */
    public function getReferenceKey()
    {
        return $this->referenceKey;
    }

    /**
     * Set pinKey
     *
     * @param string $pinKey
     */
    public function setPinKey($pinKey)
    {
        $this->pinKey = $pinKey;
    }

    /**
     * Get pinKey
     *
     * @return string $pinKey
     */
    public function getPinKey()
    {
        return $this->pinKey;
    }

    /**
     * Set status
     *
     * @param string $status
     */
    public function setStatus($status)
    {
        $this->status = $status;
    }

    /**
     * Get status
     *
     * @return string $status
     */
    public function getStatus()
    {
        return $this->status;
    }

    /**
     * Set channel
     *
     * @param string $channel
     */
    public function setChannel($channel)
    {
        $this->channel = $channel;
    }

    /**
     * Get channel
     *
     * @return string $channel
     */
    public function getChannel()
    {
        return $this->channel;
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
     * Set processingWait
     *
     * @param integer $processingWait
     */
    public function setProcessingWait($processingWait)
    {
        $this->processingWait = $processingWait;
    }

    /**
     * Get processingWait
     *
     * @return integer $processingWait
     */
    public function getProcessingWait()
    {
        return $this->processingWait;
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
     * Set finishedListenerIds
     *
     * @param string $finishedListenerIds
     */
    public function setFinishedListenerIds($finishedListenerIds)
    {
        $this->finishedListenerIds = $finishedListenerIds;
    }

    /**
     * Get finishedListenerIds
     *
     * @return string $finishedListenerIds
     */
    public function getFinishedListenerIds()
    {
        return $this->finishedListenerIds;
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

    /**
     * Returns represenation of event formatted for dispatching into the queue.
     *
     * @return stdClass
     */
    public function jsonSerialize()
    {
        // Decode $data string so that it gets properly re-encoded when included
        // in $event object.
        $data = json_decode($this->data);
        $finishedListenerIds = explode(',', $this->finishedListenerIds);

        $event = new \stdClass;
        $event->queueId             = $this->queueId;
        $event->name                = $this->event;
        $event->referenceKey        = $this->referenceKey;
        $event->pinKey              = $this->pinKey;
        $event->data                = $data;
        $event->finishedListenerIds = $finishedListenerIds;

        return $event;
    }

    /**
     * {@inheritDoc}
     */
    public function __toString()
    {
        return "{EventQueue-{$this->queueId}-{$this->event}}";
    }

    /**
     * @ORM\PrePersist
     */
    public function prePersist()
    {
        if (!isset($this->finishedListenerIds)) {
            $this->finishedListenerIds = '';
        }

        if (!isset($this->processingDuration)) {
            $this->processingDuration = 0;
        }
    }

}
