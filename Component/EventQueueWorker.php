<?php
namespace Celltrak\EventQueueBundle\Component;

use Doctrine\DBAL\DBALException;
use Doctrine\Bundle\DoctrineBundle\Registry as DoctrineRegistry;
use Doctrine\ORM\EntityManager;
use CTLib\Component\Monolog\Logger;
use CTLib\Util\PDOExceptionInspector;
use CTLib\Util\MemoryUsageMonitor;
use Celltrak\EventQueueBundle\Entity\EventQueueWorker as WorkerEntity;
use Celltrak\EventQueueBundle\Entity\EventQueue;


/**
 * Worker responsible for notifying registered listeners of queued event.
 *
 * @author Mike Turoff
 */
class EventQueueWorker
{

    /**
     * EventQueueWorker.workerId
     * @var integer
     */
    protected $workerId;

    /**
     * @var EventQueueChannel
     */
    protected $channel;

    /**
     * @var EventQueueProcessingManager
     */
    protected $processingManager;

    /**
     * @var EventQueueDispatcher
     */
    protected $eventQueueDispatcher;

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
     * Indicates whether worker has been activated.
     * @var boolean
     */
    protected $isActivated;

    /**
     * Maximum number of attempts calling an individual listener if it fails
     * due to transaction deadlock.
     * @var integer
     */
    protected $maxListenerAttempts;

    /**
     * The percentage of the PHP ini memory_limit available for a worker. Set to
     * 0 to let worker use all of the ini limit.
     * @var float
     */
    protected $memoryUsagePercentage;

    /**
     * The number of seconds a worker will sleep between each main processing
     * iteration. Set to 0 to not sleep.
     * @var integer
     */
    protected $sleepSeconds;

    /**
     * EventQueue.queueId of queue entry currently being processed by worker.
     * @var array
     */
    protected $currentEvent;

    /**
     * Timestamp of when worker started processing current queue entry.
     * @var integer
     */
    protected $currentQueueStartedOn;


    /**
     * @param integer $workerId          EventQueueWorker.workerId
     * @param EventQueueChannel $channel
     * @param EventQueueProcessingManager $processingManager
     * @param EventQueueDispatcher $eventQueueDispatcher
     * @param EntityManager $entityManager
     * @param DoctrineRegistry $doctrine
     * @param Logger $logger
     */
    public function __construct(
        $workerId,
        EventQueueChannel $channel,
        EventQueueProcessingManager $processingManager,
        EventQueueDispatcher $eventQueueDispatcher,
        EntityManager $entityManager,
        DoctrineRegistry $doctrine,
        Logger $logger
    ) {
        $this->workerId                 = $workerId;
        $this->channel                  = $channel;
        $this->processingManager        = $processingManager;
        $this->eventQueueDispatcher     = $eventQueueDispatcher;
        $this->entityManager            = $entityManager;
        $this->doctrine                 = $doctrine;
        $this->logger                   = $logger;
        $this->isActivated              = false;
        $this->currentEvent             = null;
        $this->currentQueueStartedOn    = null;
        $this->memoryMonitor            = null;
        $this->maxListenerAttempts      = 1;
        $this->sleepSeconds             = null;
        $this->memoryUsagePercentage    = null;
    }

    /**
     * Starts worker.
     * @param  string $hostname System host running this worker.
     * @param  integer $pid System process id assigned to this worker.
     * @return void
     */
    public function start($hostname, $pid)
    {
        if ($this->isActivated) {
            throw new \RuntimeException("Worker (workerId {$this->workerId}) has already been started");
        }

        $this->logger->debug("EventQueueWorker: workerId {$this->workerId}: starting with pid {$pid}");

        // This worker needs its channel to activate it before it can start.
        if (!$this->channel->activateWorker($this->workerId, $hostname, $pid)) {
            // Couldn't activate. This is probably b/c the channel is no longer
            // running.
            throw new \RuntimeException("Could not activate worker (workerId {$this->workerId})");
        }

        $this->isActivated = true;

        // Initialize MemoryUsageMonitor so this worker can continually check
        // that it doesn't hit the maximum usage threshold.
        if ($this->memoryUsagePercentage) {
            $this->memoryMonitor =
                new MemoryUsageMonitor($this->memoryUsagePercentage);
        }

        // Create a WorkerProxy used to pass to the listener callbacks so they
        // have a controlled way to inspect/interact with the worker.
        $this->workerProxy = new EventQueueWorkerProxy(
            $this->workerId,
            $this->channel,
            $this->logger,
            $this->memoryMonitor
        );

        $this->work();
    }

    /**
     * Stops worker.
     *
     * @param string $deactivatedReason The reason why this worker is stopping.
     * @param string $error The error that forced this worker to stop.
     * @return void
     */
    public function stop($deactivatedReason, $error = null)
    {
        if (
            $deactivatedReason == WorkerEntity::DEACTIVATED_REASON_ERROR
            && $this->currentEvent
        ) {
            if ($this->currentEventResult->isStopped() == false) {
                $this->currentEventResult->stopLogging($error);
            }

            try {
                $this->logEventProcessingResult(
                    $this->currentEvent,
                    $this->currentEventResult
                );
            } catch (\Exception $e) {
                $this->logger->error((string) $e);
            }
        }

        $this
            ->channel
            ->deactivateWorker($this->workerId, $deactivatedReason, $error);
        $this->isActivated = false;
    }

    /**
     * Sets $memoryUsagePercentage.
     *
     * @param float $memoryUsagePercentage
     * @return void
     */
    public function setMemoryUsagePercentage($memoryUsagePercentage)
    {
        $this->memoryUsagePercentage = $memoryUsagePercentage;
    }

    /**
     * Sets $sleepSeconds.
     *
     * @param integer $sleepSeconds
     * @return void
     */
    public function setSleepSeconds($sleepSeconds)
    {
        $this->sleepSeconds = $sleepSeconds;
    }

    /**
     * Sets $maxListenerAttempts.
     *
     * @param integer $maxListenerAttempts
     * @return void
     */
    public function setMaxListenerAttempts($maxListenerAttempts)
    {
        $this->maxListenerAttempts = $maxListenerAttempts;
    }

    /**
     * This is where th worker gets busy.
     *
     * @return void
     */
    protected function work()
    {
        // The basic workflow for the worker is:
        //
        //  Check if it's okay to continue working
        //      -> If it's NOT okay, deactivate.
        //      -> If it's okay, process the next event in the queue.
        //  Repeat indefinitely.
        //
        // Things get a little trickier because there are two types of events in
        // the queue: standard and pinned.
        //
        //  +Standard+ events have no dependencies on other events and can be
        //  processed in parallel by multiple workers.
        //
        //  +Pinned+ events are linked together by their pin key. All events for
        //  a single pin key must be processed in series so no two workers can
        //  be assigned to the same pin key at the same time.
        //
        //  The most common example of pinned events are the activity lifecycle
        //  events (e.g., start, finish, etc.) All of the lifecycle events are
        //  pinned based on the activity root so that all of the root's events
        //  are processed in order.
        //
        // Both standard and pinned events are added to the channel's queue in
        // the order they are dispatched so the worker primarily just needs to
        // continue to request for the next queue entry. However, only a pointer
        // ("the pin key pointer") is stored in the main queue for pinned events.
        // The actual pinned events are stored in individual queues dedicated to
        // each pin key. When a worker successfully acquires a pin key lock, it
        // will process all of the queued events for that pin key before releasing
        // the lock and retrieving the next entry from the main queue.
        //
        // The worker is required to check that it's okay to continue before
        // asking the channel for the next event. This is primarily to catch
        // when the worker exceeds its memory limit, but also allows for the
        // worker monitor daemon to have upmost conrol of the worker's length of
        // service.

        // Seed the processing with a NULL $deactivatedReason. Ultimately there
        // should be one when a call to isOkayToContinueWorking returns false.
        $deactivatedReason = null;

        // Make things a bit easier on the eyes.
        $channel = $this->channel;

        // Outer loop that keeps the worker running regardless of whether any
        // events exist in the queue. The EventQueueWorkerMonitor daemon is
        // currently responsible for determining when a worker is no longer
        // needed.

        while ($this->continueWorking($deactivatedReason)) {

            while ($entry = $this->getNextQueueEntry($deactivatedReason)) {

                list($isPinKeyPointer, $data) = $entry;

                if (!$isPinKeyPointer) {
                    // Just a standard event. Nice and easy.
                    // Force to associative array because listeners expect
                    // event.data to be array.
                    $event = json_decode($data, true);
                    $result = $this->processEvent($event);
                    $this->logEventProcessingResult($event, $result);
                    $this->clearEntityManagers();
                } else {
                    $pinKey = $data;

                    if (!$channel->acquirePinKeyLock($pinKey, $this->workerId)) {
                        // Another worker already owns this pin key. Retrieve
                        // the next entry in the main queue.
                        continue;
                    }

                    $this->processPinnedEvents($pinKey, $deactivatedReason);

                    // The worker will release its lock on this pin key once
                    // it has completed processing all the queued events. This
                    // worker or any other worker may process future events
                    // assigned to this pin key.
                    $channel->releasePinKeyLock($pinKey, $this->workerId);
                }
            }

            if (!$deactivatedReason && $this->sleepSeconds) {
                // Take a breather.
                $this->suspendWork($this->sleepSeconds);
            }
        }

        // All good things must come to an end.
        $this->stop($deactivatedReason);
    }

    /**
     * Suspends working for specified number of seconds.
     *
     * NOTE: When suspending work, the worker will close all open database
     * connections. It will reopen those connections after finishing its nap.
     *
     * @param integer $sleepSeconds
     * @return void
     */
    protected function suspendWork($sleepSeconds)
    {
        $this->logger->debug("EventQueueWorker: workerId {$this->workerId}: suspending work for {$sleepSeconds} second(s)");

        $dbConnections = $this->closeAllDatabaseConnections();

        sleep($sleepSeconds);

        $this->reopenDatabaseConnections($dbConnections);
    }

    /**
     * Indicates whether worker can continue working.
     * This is determined by checking that the worker has:
     *      (1) Not exceeded its memory limit
     *          AND
     *      (2) Not been killed
     *
     * @param string $deactivatedReason Set internally if worker must stop.
     * @return boolean
     */
    protected function continueWorking(&$deactivatedReason = null)
    {
        if ($deactivatedReason) {
            return false;
        }

        if ($this->hasExceededMemoryLimit()) {
            $deactivatedReason = WorkerEntity::DEACTIVATED_REASON_MEMORY;
            return false;
        }

        if ($this->isKilled()) {
            $deactivatedReason = WorkerEntity::DEACTIVATED_REASON_KILLED;
            return false;
        }

        return true;
    }

    /**
     * Indicates whether worker has reached or exceeded its allowed memory
     * limit as set by $memoryUsagePercentage.
     *
     * @return boolean
     */
    protected function hasExceededMemoryLimit()
    {
        if (
            $this->memoryMonitor
            && $this->memoryMonitor->hasExceededMemoryLimit()
        ) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * Indicates whether worker has been killed.
     *
     * @return boolean
     */
    protected function isKilled()
    {
        // Worker check-in will fail if this worker has been killed.
        if (!$this->channel->checkInWorker($this->workerId)) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * Returns next entry in queue's stack.
     *
     * @param string $deactivatedReason Set internally if worker must stop.
     * @return array [$isPinKeyPointer, $data]
     */
    protected function getNextQueueEntry(&$deactivatedReason = null)
    {
        if ($deactivatedReason) {
            return [];
        }

        if ($this->hasExceededMemoryLimit()) {
            $deactivatedReason = WorkerEntity::DEACTIVATED_REASON_MEMORY;
            return [];
        }

        $entry = $this->channel->getNextQueueEntry($this->workerId);

        if ($entry === false) {
            // Event Queue has reported that this worker has been killed.
            $deactivatedReason = WorkerEntity::DEACTIVATED_REASON_KILLED;
            return [];
        }

        return $entry;
    }

    /**
     * Returns next pinned event in queue for specified $pinKey.
     *
     * @param string $pinKey
     * @param string $deactivatedReason Set internally if worker must stop.
     * @return stdClass|null
     */
    protected function getNextPinnedEvent($pinKey, &$deactivatedReason = null)
    {
        if ($deactivatedReason) {
            return null;
        }

        if ($this->hasExceededMemoryLimit()) {
            $deactivatedReason = WorkerEntity::DEACTIVATED_REASON_MEMORY;
            return null;
        }

        $event = $this->channel->getNextPinnedEvent($pinKey, $this->workerId);

        if ($event === false) {
            // Event Queue has reported that this worker has been killed.
            $deactivatedReason = WorkerEntity::DEACTIVATED_REASON_KILLED;
            return null;
        } elseif (!$event) {
            // No more pinned events.
            return null;
        } else {
            // Force to associative array because listeners expect event.data
            // to be array.
            return json_decode($event, true);
        }
    }

    /**
     * Processes pinned events in queue for specified $pinKey.
     *
     * @param string $pinKey
     * @param string $deactivatedReason Set internally if worker must stop.
     * @return void
     */
    protected function processPinnedEvents($pinKey, &$deactivatedReason = null)
    {
        // Track whether this pin key has a blocking event. Not only
        // must pinned events be processed in order, but the #2
        // pinned event must be skipped if the #1 event failed or
        // was never processed. Setting to null indicates that it's
        // unknown whether a blockage exists so the worker will
        // query the database queue to find out.
        $this->currentPinKeyHasBlock = null;

        // Retrieve the next queued event for this pin key.
        while ($event = $this->getNextPinnedEvent($pinKey, $deactivatedReason)) {
            $result = $this->processEvent($event);
            $this->logEventProcessingResult($event, $result);
            $this->clearEntityManagers();

            if (!$result->isSuccessful()) {
                $this->currentPinKeyHasBlock = true;
            }
        }
    }

    /**
     * Processes queue event.
     *
     * @param array $event
     * @return EventQueueProcessingResult
     */
    protected function processEvent(array $event)
    {
        // Start a new thread to group all the log messages for this event.
        $this->logger->startThread();

        $queueId                = $event['queueId'];
        $eventName              = $event['name'];
        $data                   = $event['data'];
        $pinKey                 = $event['pinKey'];
        $finishedListenerIds    = $event['finishedListenerIds'];

        $this->logger->debug("EventQueueWorker: workerId {$this->workerId}: processing event for queueId {$queueId}");

        $result = new EventQueueProcessingResult(
            $queueId,
            $finishedListenerIds
        );
        $result->startLogging($this->logger->getThread());

        // Hold on to current queue id and processing result in case execution
        // triggers fatal error. If that happens, the event will get logged
        // properly in the worker's stop method.
        $this->currentEvent = $event;
        $this->currentEventResult = $result;

        if ($pinKey && $this->hasBlockingEvent($pinKey, $queueId)) {
            $this->logger->debug("EventQueueWorker: workerId {$this->workerId}: pinned event is blocked");
            $result->blocked();
            return $result;
        }

        try {
            $this->runEventPreProcessors($queueId, $eventName, $data, $pinKey);
            $this->runEventListeners($queueId, $eventName, $data, $result);
            $this->runEventPostProcessors($queueId, $eventName, $data, $pinKey, $result);
            $exception = null;
        } catch (\Exception $e) {
            $exception = (string) $e;
            $this->logger->error($exception);
        }

        $result->stopLogging($exception);

        $this->logger->stopThread();

        // Null these out so that any exception thrown after this point won't
        // be attributed to this event.
        $this->currentEvent = null;
        $this->currentEventResult = null;

        return $result;
    }

    /**
     * Runs pre-processors registered for event.
     * @param  integer $queueId   EventQueue.queueId
     * @param  string $eventName
     * @param  array  &$data
     * @param  string $pinKey
     * @return void
     */
    protected function runEventPreProcessors(
        $queueId,
        $eventName,
        array &$data,
        $pinKey
    ) {
        $processors =
            $this->processingManager->getPreProcessorsForEvent($eventName);

        foreach ($processors as $processor) {
            $processor->process($queueId, $eventName, $data, $pinKey);
        }
    }

    /**
     * Notifies registered listeners of event.
     * @param integer $queueId
     * @param string $eventName
     * @param  array  $data
     * @param EventQueueProcessingResult $eventResult
     * @return void
     */
    protected function runEventListeners(
        $queueId,
        $eventName,
        array $data = [],
        EventQueueProcessingResult $eventResult
    ) {
        $listeners = $this->processingManager->getListenersForEvent($eventName);

        foreach ($listeners as $listener) {
            $this->logger->debug("EventQueueWorker: workerId {$this->workerId}: processing {$listener}");

            if ($eventResult->isFinishedListenerId($listener->getId())) {
                $this->logger->debug("EventQueueWorker: workerId {$this->workerId}: skipping bc listener finished earlier");
                continue;
            }

            $listenerResult = new EventQueueListenerProcessingResult(
                $listener->getId()
            );
            $listenerResult->startLogging($this->logger->getThread());

            // This listener cannot run if any of its pre-requisite listeners
            // have failed.
            $requiredFailedListenerIds =
                array_intersect(
                    $listener->getRequiredListenerIds(),
                    $eventResult->getFailedListenerIds()
                );

            if ($requiredFailedListenerIds) {
                $this->logger->debug("EventQueueWorker: workerId {$this->workerId}: marking as failed bc listener has failed dependencies");

                $requiredFailedList = join(', ', $requiredFailedListenerIds);
                $exception = "Requires failed listener(s): {$requiredFailedList}";

                $listenerResult->stopLogging($exception);
                $this->logListenerProcessingResult($queueId, $listenerResult);
                $eventResult->recordListenerResult($listenerResult);
                continue;
            }

            try {
                $this->callEventListener($listener, $data);

                // Events are automatically staged in memory rather than
                // dispatched if this process has a database connection with an
                // active transaction. The staged entries are automatically
                // flushed when the EventQueueDispatcher is garbage collected,
                // but that won't occur until this worker is deactivated.
                // Instead, the worker forces the flushing of the staged entries.
                if ($this->eventQueueDispatcher->hasStagedQueueEntries()) {
                    $this->eventQueueDispatcher->flushStagedQueueEntries();
                }

                $exception = null;
            } catch (\Exception $e) {
                $exception = (string) $e;
                $this->logger->error($exception);
            }

            $listenerResult->stopLogging($exception);
            $this->logListenerProcessingResult($queueId, $listenerResult);
            $eventResult->recordListenerResult($listenerResult);
        }
    }

    /**
     * Calls individual event listener.
     * @param  callable  $listener
     * @param  array   $data
     * @param  integer $attempt          Used internally to track the number of
     *                                   attempts calling this listener. Used to
     *                                   retry after encountering db deadlock
     *                                   exception.
     * @return array                    [$processingDuration, $memoryUsageDelta]
     */
    protected function callEventListener(
        callable $listener,
        array $data,
        $attempt = 1
    ) {
        try {
            call_user_func($listener, $data, $this->workerProxy);
        } catch (DBALException $e) {
            // Test if db lock exception. If it is, we may attempt listener again.
            $pdoException = $e->getPrevious();

            if (!$pdoException || (!$pdoException instanceof \PDOException)) {
                throw $e;
            }

            $exceptionInspector = new PDOExceptionInspector($pdoException);

            if (!$exceptionInspector->isDeadlock()) {
                throw $e;
            }

            $this->logger->warn("EventQueueWorker: workerId {$this->workerId}: encountered db deadlock calling listener (attempt = {$attempt}): {$pdoException}");

            if ($attempt == $this->maxListenerAttempts) {
                throw $e;
            }

            // Retry the listener.
            $attempt += 1;
            return $this->callEventListener($listener, $data, $attempt);
        }
    }

    /**
     * Runs post-processors registered for event.
     * @param  integer $queueId      EventQueue.queueId
     * @param  string $eventName
     * @param  array  $data
     * @param  string $pinKey
     * @param  EventQueueProcessingResult $result
     * @return void
     */
    protected function runEventPostProcessors(
        $queueId,
        $eventName,
        array $data,
        $pinKey,
        EventQueueProcessingResult $result
    ) {
        $processors =
            $this->processingManager->getPostProcessorsForEvent($eventName);

        foreach ($processors as $processor) {
            $processor->process($queueId, $eventName, $data, $pinKey, $result);
        }
    }

    /**
     * Indicates whether pinned event is blocked by an earlier event assigned to
     * the same pin key.
     *
     * @param string $pinKey
     * @param integer $queueId
     * @return boolean
     */
    protected function hasBlockingEvent($pinKey, $queueId)
    {
        if (is_null($this->currentPinKeyHasBlock)) {
            $this->currentPinKeyHasBlock =
                $this
                ->entityManager
                ->getRepository('CelltrakEventQueueBundle:EventQueue')
                ->hasBlockingEventForPinKey(
                    $this->channel->getChannelId(),
                    $pinKey,
                    $queueId
                );
        }

        return $this->currentPinKeyHasBlock;
    }

    /**
     * Updates event's log record.
     *
     * @param array $event
     * @param EventQueueProcessingResult $result
     * @return void
     */
    protected function logEventProcessingResult(
        array $event,
        EventQueueProcessingResult $result
    ) {
        // Use INSERT ... ON DUPLICATE KEY UPDATE because it's most likely that
        // this event_queue entry has been added by EventQueueDispatcher when
        // dispatching the event. However, there's a remote possibility that the
        // worker received this event and processed it before the dispatcher
        // could run its INSERT into event_queue.

        $sql = "
            INSERT INTO event_queue
            (
                queue_id,
                event,
                data,
                reference_key,
                pin_key,
                status,
                channel,
                worker_id,
                processing_wait,
                processing_duration,
                exception,
                log_thread,
                added_on,
                modified_on
            )
            VALUES
            (
                :queueId,
                :event,
                :data,
                :referenceKey,
                :pinKey,
                :status,
                :channel,
                :workerId,
                :processingWait,
                :processingDuration,
                :exception,
                :logThread,
                :now,
                :now
            )
            ON DUPLICATE KEY UPDATE
                worker_id               = :workerId,
                status                  = :status,
                processing_wait         = IF(
                                            processing_wait IS NULL,
                                            :startedProcessingOn - added_on,
                                            processing_wait),
                processing_duration     = processing_duration + :processingDuration,
                exception               = :exception,
                log_thread              = :logThread,
                modified_on             = :now";

        if ($result->hasProcessedListeners() == false) {
            $status = EventQueue::STATUS_NO_LISTENERS;
        } elseif ($result->isSuccessful()) {
            $status = EventQueue::STATUS_FINISHED;
        } elseif ($result->isBlocked()) {
            $status = EventQueue::STATUS_SKIPPED;
        } else {
            $status = EventQueue::STATUS_EXCEPTION_OCCURRED;
        }

        $data = json_encode($event['data']);

        $params = [
            'queueId'               => $event['queueId'],
            'event'                 => $event['name'],
            'data'                  => $data,
            'referenceKey'          => $event['referenceKey'],
            'pinKey'                => $event['pinKey'],
            'status'                => $status,
            'channel'               => $this->channel->getChannelId(),
            'workerId'              => $this->workerId,
            'processingWait'        => 0,
            'processingDuration'    => $result->getTotalProcessingDuration(),
            'exception'             => $result->getException(),
            'logThread'             => $result->getLogThread(),
            'now'                   => time(),
            'startedProcessingOn'   => $result->getStartTime()
        ];

        $conn = $this->entityManager->getConnection();

        // Ensure database connection is still open. It may have closed if a
        // listener took a very long time to complete.
        if ($conn->isConnected() == false) {
            $conn->connect();
        }

        try {
            $conn->executeQuery($sql, $params);
        } catch (DBALException $e) {
            // Test if db lock exception. If it is, we'll retry insert.
            $pdoException = $e->getPrevious();

            if (!$pdoException || !($pdoException instanceof \PDOException)) {
                throw $e;
            }

            $exceptionInspector = new PDOExceptionInspector($pdoException);

            if (!$exceptionInspector->isDeadlock()) {
                throw $e;
            }

            $this->logger->warn("EventQueueWorker: workerId {$this->workerId}: encountered db deadlock logging {$result}: {$pdoException}");

            // It's really important that the result is logged. Keep trying.
            return $this->logEventProcessingResult($event, $result);
        }
    }

    /**
     * Logs event listener result.
     *
     * @param string $queueId
     * @param EventQueueListenerProcessingResult $result
     * @return void
     */
    protected function logListenerProcessingResult(
        $queueId,
        EventQueueListenerProcessingResult $result
    ) {
        $sql = "
            INSERT INTO event_queue_listener_log
            (
                queue_id,
                listener_id,
                worker_id,
                is_finished,
                processing_duration,
                memory_usage_delta,
                exception,
                log_thread,
                added_on
            )
            VALUES
            (
                :queueId,
                :listenerId,
                :workerId,
                :isFinished,
                :processingDuration,
                :memoryUsageDelta,
                :exception,
                :logThread,
                :now
            )";

        $isFinished = is_null($result->getException());

        $params = [
            'queueId'           => $queueId,
            'listenerId'        => $result->getListenerId(),
            'workerId'          => $this->workerId,
            'isFinished'        => $isFinished,
            'processingDuration'=> $result->getProcessingDuration(),
            'memoryUsageDelta'  => $result->getMemoryUsageDelta(),
            'exception'         => $result->getException(),
            'logThread'         => $result->getLogThread(),
            'now'               => time()
        ];

        $conn = $this->entityManager->getConnection();

        // Ensure database connection is still open. It may have closed if a
        // listener took a very long time to complete.
        if ($conn->isConnected() == false) {
            $conn->connect();
        }

        try {
            $conn->executeQuery($sql, $params);
        } catch (DBALException $e) {
            // Test if db lock exception. If it is, we'll retry insert.
            $pdoException = $e->getPrevious();

            if (!$pdoException || !($pdoException instanceof \PDOException)) {
                throw $e;
            }

            $exceptionInspector = new PDOExceptionInspector($pdoException);

            if (!$exceptionInspector->isDeadlock()) {
                throw $e;
            }

            $this->logger->warn("EventQueueWorker: workerId {$this->workerId}: encountered db deadlock logging {$result}: {$pdoException}");

            // It's really important that the result is logged. Keep trying.
            return $this->logListenerProcessingResult($queueId, $result);
        }
    }

    /**
     * Clears all open EntityManagers to reduce memory footprint.
     *
     * @return void
     */
    protected function clearEntityManagers()
    {
        foreach ($this->doctrine->getManagers() as $name => $entityManager) {
            if ($entityManager->isOpen()) {
                $entityManager->clear();
            }
        }
    }

    /**
     * Closes all open database connections.
     *
     * @return array  Enumerated array of closed Connection instances.
     */
    protected function closeAllDatabaseConnections()
    {
        $closedDatabaseConnections = [];

        foreach ($this->doctrine->getConnections() as $name => $connection) {
            if ($connection->isConnected()) {
                $connection->close();
                $closedDatabaseConnections[] = $connection;
            }
        }

        return $closedDatabaseConnections;
    }

    /**
     * Reopens database connections.
     *
     * @param array $closedDatabaseConnections  Expects enumerated array of
     *                                          Connection instances.
     * @return void
     */
    protected function reopenDatabaseConnections(array $closedDatabaseConnections)
    {
        foreach ($closedDatabaseConnections as $connection) {
            $connection->connect();
        }
    }

}
