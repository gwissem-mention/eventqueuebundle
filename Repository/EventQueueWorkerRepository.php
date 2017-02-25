<?php
namespace CellTrak\EventQueueBundle\Repository;

use CellTrak\EventQueueBundle\Entity\EventQueueWorker;


/**
 * EventQueueWorker Repository class.
 *
 * @author Mike Turoff
 */
class EventQueueWorkerRepository extends \CTLib\Repository\BaseRepository
{

    /**
     * Finds active workers.
     * @return DetachedEntityIterator
     */
    public function _findActiveWorkers($channel=null)
    {
        $criteria = [
            'status' => EventQueueWorker::STATUS_ACTIVE
        ];

        if ($channel) {
            $criteria['channel'] = $channel;
        }

        return $this->_findBy($criteria);
    }

    /**
     * Finds active workers that haven't reported since specified time.
     * @param  integer $lastReportedOn timestamp
     * @param  string $channel
     * @return DetachedEntityIterator
     */
    public function _findStuckWorkers($lastReportedOn, $channel=null)
    {
        $validStatuses = [
            EventQueueWorker::STATUS_ACTIVE,
            EventQueueWorker::STATUS_REGISTERED,
            EventQueueWorker::STATUS_SUSPENDED
        ];

        $qbr = $this
                ->createDetachedQueryBuilder('w')
                ->where(
                    'w.status           IN (:validStatuses)',
                    'w.lastReportedOn   < :lastReportedOn')
                ->setParameters([
                    'validStatuses'     => $validStatuses,
                    'lastReportedOn'    => $lastReportedOn]);

        if ($channel) {
            $qbr
                ->andWhere('w.channel = :channel')
                ->setParameter('channel', $channel);
        }

        $results = $qbr->getQuery()->getResult();
        return $this->createDetachedEntityIterator($results);
    }


}
