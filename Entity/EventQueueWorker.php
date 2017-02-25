<?php
namespace Celltrak\EventQueueBundle\Entity;

use Doctrine\ORM\Mapping as ORM,
    CTLib\Entity\BaseEntity;

/**
 * EventQueueWorker.php
 *
 * @author Mike Turoff
 *
 * @ORM\Table(name="event_queue_worker")
 * @ORM\Entity(repositoryClass="Celltrak\EventQueueBundle\Repository\EventQueueWorkerRepository")
 */
class EventQueueWorker extends BaseEntity
{

    /**
     * Status codes.
     */
    const STATUS_PROVISIONING           = 'P';
    const STATUS_FAILED_PROVISIONING    = 'X';
    const STATUS_ACTIVE                 = 'A';
    const STATUS_DEACTIVATED            = 'D';

    /**
     * Deactivated reason codes.
     */
    const DEACTIVATED_REASON_ERROR      = 'E';
    const DEACTIVATED_REASON_MEMORY     = 'M';
    const DEACTIVATED_REASON_KILLED     = 'K';
    const DEACTIVATED_REASON_ZOMBIE     = 'Z';



    /**
     * @var integer $workerId
     *
     * @ORM\Column(name="worker_id", type="integer", nullable=false)
     * @ORM\Id
     * @ORM\GeneratedValue(strategy="AUTO")
     */
    private $workerId;

    /**
     * @var string $channel
     *
     * @ORM\Column(name="channel", type="string", length=20, nullable=false)
     */
    private $channel;

    /**
     * @var string $status
     *
     * @ORM\Column(name="status", type="string", length=1, nullable=false)
     */
    private $status;

    /**
     * @var string $hostName
     *
     * @ORM\Column(name="host_name", type="string", length=50, nullable=true)
     */
    private $hostName;

    /**
     * @var integer $pid
     *
     * @ORM\Column(name="pid", type="integer", nullable=true)
     */
    private $pid;

    /**
     * @var integer $activatedOn
     *
     * @ORM\Column(name="activated_on", type="integer", nullable=true)
     */
    private $activatedOn;

    /**
     * @var integer $deactivatedOn
     *
     * @ORM\Column(name="deactivated_on", type="integer", nullable=true)
     */
    private $deactivatedOn;

    /**
     * @var string $deactivatedReason
     *
     * @ORM\Column(name="deactivated_reason", type="string", length=1, nullable=true)
     */
    private $deactivatedReason;

    /**
     * @var text $exception
     *
     * @ORM\Column(name="exception", type="text", nullable=true)
     */
    private $exception;

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
     * Set hostName
     *
     * @param string $hostName
     */
    public function setHostName($hostName)
    {
        $this->hostName = $hostName;
    }

    /**
     * Get hostName
     *
     * @return string $hostName
     */
    public function getHostName()
    {
        return $this->hostName;
    }

    /**
     * Set pid
     *
     * @param integer $pid
     */
    public function setPid($pid)
    {
        $this->pid = $pid;
    }

    /**
     * Get pid
     *
     * @return integer $pid
     */
    public function getPid()
    {
        return $this->pid;
    }

    /**
     * Set activatedOn
     *
     * @param integer $activatedOn
     */
    public function setActivatedOn($activatedOn)
    {
        $this->activatedOn = $activatedOn;
    }

    /**
     * Get activatedOn
     *
     * @return integer $activatedOn
     */
    public function getActivatedOn()
    {
        return $this->activatedOn;
    }

    /**
     * Set deactivatedOn
     *
     * @param integer $deactivatedOn
     */
    public function setDeactivatedOn($deactivatedOn)
    {
        $this->deactivatedOn = $deactivatedOn;
    }

    /**
     * Get deactivatedOn
     *
     * @return integer $deactivatedOn
     */
    public function getDeactivatedOn()
    {
        return $this->deactivatedOn;
    }

    /**
     * Set deactivatedReason
     *
     * @param string $deactivatedReason
     */
    public function setDeactivatedReason($deactivatedReason)
    {
        $this->deactivatedReason = $deactivatedReason;
    }

    /**
     * Get deactivatedReason
     *
     * @return string $deactivatedReason
     */
    public function getDeactivatedReason()
    {
        return $this->deactivatedReason;
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

    public function __toString()
    {
        return "{EventQueueWorker-{$this->workerId}}";
    }


}
