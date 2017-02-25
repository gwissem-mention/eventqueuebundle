<?php
namespace Celltrak\EventQueueBundle\Repository;

use Celltrak\EventQueueBundle\Entity\EventQueue;

/**
 * EventQueueRepository
 *
 * @author Mike Turoff
 */
class EventQueueRepository extends \CTLib\Repository\BaseRepository
{

    /**
     * Finds max queueId of channel events that require re-dispatch.
     *
     * @param string $channel
     * @return integer
     */
    public function findChannelMaxQueueIdRequiringReDispatch($channel)
    {
        $validStatuses = [
            EventQueue::STATUS_PENDING,
            EventQueue::STATUS_NOT_DISPATCHED
        ];

        $queryBuilder =
            $this
            ->createQueryBuilder('q')
            ->select('MAX(q.queueId)')
            ->where(
                'q.channel  = :channel',
                'q.status   IN (:validStatuses)'
            )
            ->setParameters([
                'channel'       => $channel,
                'validStatuses' => $validStatuses
            ]);

        return $queryBuilder->getQuery()->getSingleScalarResult();
    }

    /**
     * Finds channel events requiring re-dispatch.
     *
     * @param string $channel
     * @param integer $maxQueueId
     * @param string $pinKey
     * @param integer $batchSize
     *
     * @return DetachedEntityIterator
     */
    public function _findChannelEventsRequiringReDispatch(
        $channel,
        $maxQueueId,
        $pinKey = null,
        $batchSize = null
    ) {
        // Using raw SQL to easily incorporate GROUP_CONCAT of finished
        // listener ids.
        $sql = "
            SELECT
                q.queue_id AS queueId,
                q.event,
                q.data,
                q.reference_key AS referenceKey,
                q.pin_key AS pinKey,
                q.status,
                q.channel,
                GROUP_CONCAT(l.listener_id SEPARATOR ',') AS finishedListenerIds
            FROM
                event_queue q
                LEFT JOIN event_queue_listener_log l
                    ON q.queue_id = l.queue_id
                    AND l.is_finished = 1
            WHERE
                q.channel       = :channel
                AND q.queue_id  <= :maxQueueId
                AND q.status    = '" . EventQueue::STATUS_PENDING . "'";

        $params = [
            'channel'       => $channel,
            'maxQueueId'    => $maxQueueId
        ];

        if ($pinKey) {
            $sql .= " AND q.pin_key = :pinKey";
            $params['pinKey'] = $pinKey;
        }

        $sql .= "
            GROUP BY
                q.queue_id
            ORDER BY
                q.queue_id DESC";

        if ($batchSize) {
            $sql .= " LIMIT {$batchSize}";
        }

        $conn = $this->getEntityManager()->getConnection();
        $results = $conn->fetchAll($sql, $params);
        return $this->createDetachedEntityIterator($results);
    }

    /**
     * Changes status of channel's re-dispatch events back to pending.
     *
     * @param string $channel
     * @param integer $maxQueueId
     *
     * @return integer  Number of affected records.
     */
    public function flagChannelReDispatchEventsAsPending(
        $channel,
        $maxQueueId = null
    ) {
        $validStatuses = [
            EventQueue::STATUS_PENDING,
            EventQueue::STATUS_NOT_DISPATCHED
        ];

        $queryBuilder =
            $this
            ->createQueryBuilder('q')
            ->update()
            ->set('q.status', ':pendingStatus')
            ->set('q.modifiedOn', ':now')
            ->where(
                'q.channel  = :channel',
                'q.status   IN (:validStatuses)'
            )
            ->setParameters([
                'pendingStatus' => EventQueue::STATUS_PENDING,
                'now'           => time(),
                'channel'       => $channel,
                'validStatuses' => $validStatuses
            ]);

        if ($maxQueueId) {
            $queryBuilder
                ->andWhere('q.queueId <= :maxQueueId')
                ->setParameter('maxQueueId', $maxQueueId);
        }

        return $queryBuilder->getQuery()->execute();
    }

    /**
     * Finds max queueId of pinned events for pin key that need to be retried
     * after one failed.
     *
     * @param string $channel
     * @param string $pinKey
     * @return integer
     */
    public function findPinKeyMaxQueueIdForRetry($channel, $pinKey)
    {
        $validStatuses = [
            EventQueue::STATUS_EXCEPTION_OCCURRED,
            EventQueue::STATUS_SKIPPED,
            EventQueue::STATUS_PENDING,
            EventQueue::STATUS_NOT_DISPATCHED
        ];

        $queryBuilder =
            $this
            ->createQueryBuilder('q')
            ->select('MAX(q.queueId)')
            ->where(
                'q.channel  = :channel',
                'q.pinKey   = :pinKey',
                'q.status   IN (:validStatuses)'
            )
            ->setParameters([
                'channel'       => $channel,
                'pinKey'        => $pinKey,
                'validStatuses' => $validStatuses
            ]);

        return $queryBuilder->getQuery()->getSingleScalarResult();
    }

    /**
     * Flags pinned events for pin key that need to be retried with PENDING
     * status.
     *
     * @param string $channel
     * @param string $pinKey
     * @param integer $maxQueueId
     *
     * @return integer  Number of affected rows.
     */
    public function flagPinKeyRetryEventsAsPending(
        $channel,
        $pinKey,
        $maxQueueId = null
    ) {
        $validStatuses = [
            EventQueue::STATUS_EXCEPTION_OCCURRED,
            EventQueue::STATUS_SKIPPED,
            EventQueue::STATUS_PENDING,
            EventQueue::STATUS_NOT_DISPATCHED
        ];

        $queryBuilder =
            $this
            ->createQueryBuilder('q')
            ->update()
            ->set('q.status', ':pendingStatus')
            ->set('q.modifiedOn', ':now')
            ->where(
                'q.channel      = :channel',
                'q.pinKey       = :pinKey',
                'q.status       IN (:validStatuses)'
            )
            ->setParameters([
                'pendingStatus' => EventQueue::STATUS_PENDING,
                'now'           => time(),
                'channel'       => $channel,
                'pinKey'        => $pinKey,
                'validStatuses' => $validStatuses
            ]);

        if ($maxQueueId) {
            $queryBuilder
                ->andWhere('q.queueId <= :maxQueueId')
                ->setParameter('maxQueueId', $maxQueueId);
        }

        return $queryBuilder->getQuery()->execute();
    }

    /**
     * Indicates whether pin key has blocking event.
     *
     * @param string $channel
     * @param string $pinKey
     * @param integer $queueId  Will only look for events before this queueId.
     *
     * @return boolean
     */
    public function hasBlockingEventForPinKey($channel, $pinKey, $queueId)
    {
        $validStatuses = [
            EventQueue::STATUS_EXCEPTION_OCCURRED,
            EventQueue::STATUS_SKIPPED,
            EventQueue::STATUS_NOT_DISPATCHED,
            EventQueue::STATUS_PENDING
        ];

        $queryBuilder =
            $this
            ->createQueryBuilder('q')
            ->select('q.queueId')
            ->where(
                'q.channel  = :channel',
                'q.pinKey   = :pinKey',
                'q.status   IN (:validStatuses)',
                'q.queueId  < :queueId'
            )
            ->setMaxResults(1)
            ->setParameters([
                'channel'       => $channel,
                'pinKey'        => $pinKey,
                'validStatuses' => $validStatuses,
                'queueId'       => $queueId
            ]);

        $results = $queryBuilder->getQuery()->getResult();
        return $results ? true : false;
    }


}
