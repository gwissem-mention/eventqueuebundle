<?php
namespace CellTrak\EventQueueBundle\Entity;

use Doctrine\ORM\Mapping as ORM,
    CTLib\Entity\BaseEntity;

/**
 * EventQueueId.php
 *
 * @author Sean Hunter
 *
 * @ORM\Table(name="event_queue_id")
 * @ORM\Entity(repositoryClass="CTLib\Repository\BaseRepository")
 */
class EventQueueId extends BaseEntity
{
    /**
     * @var integer $queueId
     *
     * @ORM\Column(name="queue_id", type="integer", nullable=false)
     * @ORM\Id
     * @ORM\GeneratedValue(strategy="AUTO")
     */
    private $queueId;

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
   
}