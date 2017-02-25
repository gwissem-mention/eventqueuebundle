<?php
namespace Celltrak\EventQueueBundle\Component;

use Doctrine\ORM\EntityManager;
use Doctrine\Bundle\DoctrineBundle\Registry as DoctrineRegistry;
use CTLib\Component\Monolog\Logger;
use Celltrak\EventQueueBundle\Entity\EventQueue;
use CTLib\Util\Util;
use Doctrine\DBAL\DBALException;
use CTLib\Util\PDOExceptionInspector;


/**
 * Dispatches events into the queue.
 *
 * @author Mike Turoff
 */
class EventQueueDispatcher
{

    /**
     * @var EventQueueManager
     */
    protected $eventQueueManager;

    /**
     * @var EntityManager
     */
    protected $entityManager;

    /**
     * @var DoctrineRegistry
     */
    protected $doctrine;

    /**
     * @var Logger
     */
    protected $logger;

    /**
     * The queue entries staged for full dispatch after the database connection
     * transactions are closed.
     * @var array
     */
    protected $stagedQueueEntries;


    /**
     * @param EventQueueManager $eventQueueManager
     * @param EntityManager $entityManager
     * @param DoctrineRegistry $doctrine
     * @param Logger $logger
     */
    public function __construct(
        EventQueueManager $eventQueueManager,
        EntityManager $entityManager,
        DoctrineRegistry $doctrine,
        Logger $logger
    ) {
        $this->eventQueueManager    = $eventQueueManager;
        $this->entityManager        = $entityManager;
        $this->doctrine             = $doctrine;
        $this->logger               = $logger;
        $this->stagedQueueEntries   = [];
    }

    /**
     * Flushes all staged queue entries.
     */
    public function __destruct()
    {
        $this->flushStagedQueueEntries();
    }

    /**
     * Dispatches event into the queue.
     * @param  string $eventName
     * @param  array  $data
     * @param  string $referenceKey
     * @param  string $pinKey
     * @return EventQueue
     */
    public function dispatch(
        $eventName,
        array $data = [],
        $referenceKey = null,
        $pinKey = null
    ) {
        $this->logger->debug("EventQueueDispatcher: dispatch '{$eventName}' event");

        // Begin database queue entry for this event. The event will always be
        // saved here, but the actual queue processing works from the
        // in-memory (Redis) queue.

        // Retrieve the next identifier for this queue entry. While this ID is
        // an auto-incremented value managed by the database, it's coming from
        // a separate table than event_queue. This awkward design solves the
        // conflicting nature of what the dispatcher must accomplish. On the
        // one hand, the dispatcher must INSERT the event into the database
        // "queue". But it also must add the event into the Redis queue, which
        // is where the actual operation of the event queue occurs. There needs
        // to be a unique ID linking the event in the database to the one
        // added to Redis. This could have been handled with a GUID, but the
        // channel [re]start operation on the queue requires a numeric sequence
        // in the database queue to rebuild the Redis one. But this still doesn't
        // answer why event_queue.queue_id isn't an auto-increment. That's because
        // the channel [re]start operation temporarily blocks dispatch in order to
        // rebuild from the database without creating duplicate events in Redis.
        // To make this work, the event doesn't get persisted to the database
        // queue until after being safely added to the Redis queue. If the
        // auto-incremented queue_id came from EventQueue, the dispatcher would
        // have to insert into event_queue before adding to Redis. This would
        // create a race condition where channel [re]start could end up
        // re-dispatching events that are already dispatched here.
        $queueId = $this->getNextQueueId();

        $queueEntry = new EventQueue([
            'queueId'       => $queueId,
            'event'         => $eventName,
            'data'          => json_encode($data),
            'referenceKey'  => $referenceKey,
            'pinKey'        => $pinKey
        ]);

        $this->logger->debug("EventQueueDispatcher: event persisted to on-disk queue as {$queueEntry}");

        // Check whether this event is handled by a channel. It's acceptable that
        // an event isn't handled by any channel and therefor doesn't have any
        // registered listeners.
        $channel = $this->eventQueueManager->getChannelForEvent($eventName);

        if (!$channel) {
            $this->logger->debug("EventQueueDispatcher: no channel configured for '{$eventName}' event");
            $queueEntry->setStatus(EventQueue::STATUS_NO_CHANNEL);
            $this->entityManager->insert($queueEntry);
            return $queueEntry;
        }

        // Update event with its assigned channel.
        $queueEntry->setChannel($channel->getChannelId());

        // Always stage queue entry so it maintains a proper sequence with any
        // existing staged entries. If there's no active database transaction,
        // flush all the staged entries (including this event) into the
        // channel queues.
        $this->stagedQueueEntries[] = $queueEntry;

        if (!$this->hasActiveDatabaseTransaction()) {
            $this->flushStagedQueueEntries();
        }

        return $queueEntry;
    }

    /**
     * Flushes all staged queue entries by adding them to channel queues.
     * @return void
     */
    public function flushStagedQueueEntries()
    {
        while ($queueEntry = array_shift($this->stagedQueueEntries)) {
            $this->addToChannelQueue($queueEntry);
        }
    }

    /**
     * Indicates whether there are any staged queue entries.
     * @return boolean
     */
    public function hasStagedQueueEntries()
    {
        return $this->stagedQueueEntries ? true : false;
    }

    /**
     * Clear all staged queue entries.
     *
     * @return void
     */
    public function clearStagedQueueEntries()
    {
        unset($this->stagedQueueEntries);
        $this->stagedQueueEntries = [];
    }

    /**
     * Clear the last staged queue entry.
     *
     * @return void
     */
    public function clearLastStagedQueueEntry()
    {
        array_pop($this->stagedQueueEntries);
    }

    /**
     * Returns next queue ID.
     * @return integer
     */
    protected function getNextQueueId()
    {
        $conn = $this->entityManager->getConnection();
        $conn->insert('event_queue_id', []);
        return $conn->lastInsertId();
    }

    /**
     * Adds queue entry to channel queue (Redis).
     * @param EventQueue $queueEntry
     * @return void
     */
    protected function addToChannelQueue(EventQueue $queueEntry)
    {
        $this->logger->debug("EventQueueDispatcher: adding {$queueEntry} to channel queue (Redis)");

        $eventName  = $queueEntry->getEvent();
        $pinKey     = $queueEntry->getPinKey();
        $channelId  = $queueEntry->getChannel();
        $channel    = $this->eventQueueManager->getChannel($channelId);

        if (!$channel) {
            $this->logger->debug("EventQueueDispatcher: no channel handling {$queueEntry}");
            return;
        }

        // Serialize EventQueue entity into event string used to dispatch into
        // Redis queue.
        $event = json_encode($queueEntry);

        try {
            if ($pinKey) {
                $added = $channel->addPinnedEvent($event, $pinKey);
            } else {
                $added = $channel->addEvent($event);
            }
        } catch (\Exception $e) {
            $this->logger->error((string) $e);
            $added = false;
        }

        if ($added) {
            $queueEntry->setStatus(EventQueue::STATUS_PENDING);
        } else {
            $queueEntry->setStatus(EventQueue::STATUS_NOT_DISPATCHED);
        }

        try {
            $this->entityManager->insert($queueEntry);
        } catch (DBALException $e) {
            // Check for duplicate key. If it is, this means that the worker
            // processed and logged lightning quick. This is very unlikely.
            $pdoException = $e->getPrevious();

            if (!$pdoException || !($pdoException instanceof \PDOException)) {
                throw $e;
            }

            $exceptionInspector = new PDOExceptionInspector($pdoException);

            if (!$exceptionInspector->isDuplicateKey()) {
                throw $e;
            }
        }
    }

    /**
     * Indicates whether this process has a database connection with an active
     * transaction.
     * @return boolean
     */
    protected function hasActiveDatabaseTransaction()
    {
        foreach ($this->doctrine->getConnections() as $connection) {
            if ($connection->isConnected()
                && $connection->isTransactionActive()
            ) {
                return true;
            }
        }

        return false;
    }

}
