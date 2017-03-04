<?php
namespace Celltrak\EventQueueBundle\Component;

use CTLib\Component\GarbageCollector\GarbageCollectorInterface;
use CTLib\Component\GarbageCollector\GarbageCollectionDateCalculator;
use Doctrine\ORM\EntityManager;
use CTLib\Component\Monolog\Logger;
use Celltrak\EventQueueBundle\Entity\EventQueue;
use Celltrak\EventQueueBundle\Entity\EventQueueWorker;

/**
 * Garbage collector for Event Queue.
 * @author Mike Turoff
 */
class EventQueueGarbageCollector implements GarbageCollectorInterface
{

    /**
     * Number of records to DELETE in a single query.
     */
    const GARBAGE_COLLECTION_BATCH_LIMIT = 1000;

    /**
     * Valid statuses when purging EventQueue entries.
     */
    const EVENT_PURGE_STATUSES = [
        EventQueue::STATUS_FINISHED,
        EventQueue::STATUS_NO_LISTENERS,
        EventQueue::STATUS_NO_CHANNEL
    ];

    /**
     * Valid statuses when purging EventQueueWorker entries.
     */
    const WORKER_PURGE_STATUSES = [
        EventQueueWorker::STATUS_PROVISIONING,
        EventQueueWorker::STATUS_FAILED_PROVISIONING,
        EventQueueWorker::STATUS_DEACTIVATED
    ];


    /**
     * @var integer
     * Number of days for database log records to live.
     */
    protected $ttlDays;

    /**
     * @var EntityManager
     */
    protected $entityManager;

    /**
     * @var Logger
     */
    protected $logger;


    /**
     * @param integer $ttlDays
     * @param EntityManager $entityManager
     * @param Logger $logger
     */
    public function __construct(
        $ttlDays,
        EntityManager $entityManager,
        Logger $logger
    ) {
        $this->ttlDays = $ttlDays;
        $this->entityManager = $entityManager;
        $this->logger = $logger;
    }

    /**
     * {@inheritDoc}
     */
    public function collectGarbage(GarbageCollectionDateCalculator $calculator)
    {
        $gcTime = $calculator->getGarbageCollectionTime($this->ttlDays);

        $totalPurged =
            $this->purgeEventQueue($purgeTime)
            + $this->purgeEventQueueWorkers($purgeTime)
            + $this->purgeEventQueueIds();

        return $totalPurged;
    }

    /**
     * Purges event queue of old, not-needed records.
     *
     * @param  integer $purgeTime
     *
     * @return integer            Number of purged records.
     */
    protected function purgeEventQueue($purgeTime)
    {
        $purgeStatusList = "'" . join("','", self::EVENT_PURGE_STATUSES) . "'";
        $totalPurged = 0;
        $conn = $this->entityManager->getConnection();

        // @TODO Find alternative to using TEMPORARY TABLE.
        $sql = "
            CREATE TEMPORARY TABLE _temp.queue_id
            (
                queue_id INT UNSIGNED NOT NULL PRIMARY KEY
            )";
        $conn->executeQuery($sql);

        do {
            $sql = "
                INSERT INTO
                    _temp.queue_id
                SELECT
                    queue_id
                FROM
                    event_queue
                WHERE
                    status IN ({$purgeStatusList})
                    AND modified_on < :purgeTime
                LIMIT " . self::GARBAGE_COLLECTION_BATCH_LIMIT;
            $params = ['purgeTime' => $purgeTime];
            $batchSize = $conn->executeUpdate($sql, $params);

            if ($batchSize === 0) {
                break;
            }

            $sql = "
                DELETE q
                FROM
                    _temp.queue_id tmp,
                    event_queue q
                WHERE
                    tmp.queue_id = q.queue_id";
            $conn->executeQuery($sql);

            $sql = "
                DELETE l
                FROM
                    _temp.queue_id tmp,
                    event_queue_listener_log l
                WHERE
                    tmp.queue_id = l.queue_id";
            $conn->executeQuery($sql);

            $sql = "TRUNCATE TABLE _temp.queue_id";
            $conn->executeQuery($sql);

            $totalPurged += $batchSize;

        } while ($batchSize == self::GARBAGE_COLLECTION_BATCH_LIMIT);

        $sql = "DROP TEMPORARY TABLE _temp.queue_id";
        $conn->executeQuery($sql);

        return $totalPurged;
    }

    /**
     * Purges event queue workers of old, not-needed records.
     *
     * @param  integer $purgeTime
     *
     * @return integer            Number of purged records.
     */
    protected function purgeEventQueueWorkers($purgeTime)
    {
        $purgeStatusList = "'" . join("','", self::WORKER_PURGE_STATUSES) . "'";
        $totalPurged = 0;

        do {
            $numberDeleted = $this
                ->entityManager
                ->createQueryBuilder()
                ->delete('CelltrakEventQueueBundle:EventQueueWorker', 'eqw')
                ->where(
                    "eqw.status IN ({$purgeStatusList})",
                    'eqw.modifiedOn < :purgeTime')
                ->setParameter('purgeTime', $purgeTime)
                ->setMaxResults(self::GARBAGE_COLLECTION_BATCH_LIMIT)
                ->getQuery()
                ->execute();

            $totalPurged += $numberDeleted;

        } while ($numberDeleted == self::GARBAGE_COLLECTION_BATCH_LIMIT);

        return $totalPurged;
    }

    /**
     * Purges event queue Ids of old, not-needed records.
     * This table is used for an Autoincrement tracker, and can be
     * cleaned out when this runs
     *
     * @return integer              Number of purged records.
     */
    protected function purgeEventQueueIds()
    {
        $totalPurged = 0;

        do {
            $numberDeleted = $this
                ->entityManager
                ->createQueryBuilder()
                ->delete('CelltrakEventQueueBundle:EventQueueId', 'eqi')
                ->setMaxResults(self::GARBAGE_COLLECTION_BATCH_LIMIT)
                ->getQuery()
                ->execute();

            $totalPurged += $numberDeleted;

        } while ($numberDeleted == self::GARBAGE_COLLECTION_BATCH_LIMIT);

        return $totalPurged;
    }

}
