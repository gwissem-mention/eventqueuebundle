<?php
namespace Celltrak\EventQueueBundle\Component;

use Doctrine\ORM\EntityManager;
use CTLib\Component\Monolog\Logger;
use Celltrak\EventQueueBundle\Entity\EventQueueWorker as WorkerEntity;
use Celltrak\EventQueueBundle\Entity\EventQueue;
use CTLib\Util\Util;
use Celltrak\RedisBundle\Component\Client\CelltrakRedis;


/**
 * Implements the controls required to manage the event queue channel.
 *
 * @author Mike Turoff
 */
class EventQueueChannel
{

    /**
     * Redis key for global active channels set.
     */
    const GLOBAL_ACTIVE_CHANNELS_KEY    = 'event_queue:active_channels';

    /**
     * Delimiter for qualified channel ID (siteId{delim}channelId).
     */
    const QUALIFIED_CHANNEL_ID_DELIM    = '#';

    /**
     * Channel state attributes.
     */
    const STATE_STATUS                  = 'status';
    const STATE_PENDING_EVENT_COUNT     = 'pendingEventCount';
    const STATE_MAX_LOAD_COUNT          = 'maxLoadCount';
    const STATE_MAX_WORKER_COUNT        = 'maxWorkerCount';
    const STATE_RUNNING_SINCE           = 'runningSince';

    /**
     * Channel statuses.
     */
    const STATUS_PRE_START              = 'PRE_START';
    const STATUS_STARTING               = 'STARTING';
    const STATUS_RUNNING                = 'RUNNING';
    const STATUS_STOPPING               = 'STOPPING';
    const STATUS_STOPPED                = 'STOPPED';

    /**
     * Prefix for pin key pointer token.
     * Used to distinguish pointers from standard events in the queue.
     */
    const PIN_KEY_POINTER_PREFIX        = '@';

    const PIN_KEY_CONTROL_LOCK_DISPATCH = 'DISPATCH';
    const PIN_KEY_CONTROL_LOCK_WORK     = 'WORK';

    /**
     * The maximum number of events needing to be re-dispatched to query for in
     * a single batch.
     */
    const RE_DISPATCH_EVENT_BATCH_SIZE  = 500;

    /**
     * The number of seconds before a process' control lock of this channel
     * will expire.
     */
    const CONTROL_LOCK_TTL              = 600;

    /**
     * The default number of seconds since a worker's last check-in before it's
     * considered to be a zombie process.
     */
    const DEFAULT_ZOMBIE_WORKER_IDLE_SECONDS = 180;


    /**
     * Channel's unique identifier.
     * @var string
     */
    protected $channelId;

    /**
     * The default number of workers allowed to run concurrently for this
     * channel.
     * @var integer
     */
    protected $defaultMaxWorkers;

    /**
     * The default number of pending events per worker (on average) before this
     * channel can create a new worker.
     * @var integer
     */
    protected $defaultMaxLoad;

    /**
     * @var CelltrakRedis
     */
    protected $redis;

    /**
     * @var EntityManager
     */
    protected $entityManager;

    /**
     * @var Logger
     */
    protected $logger;

    /**
     * The token used to run the event queue in multi-tenant mode.
     * @var string
     */
    protected $tenantKey;

    /**
     * The number of seconds since a worker's last check-in before it's
     * considered to be a zombie process.
     * @var integer
     */
    protected $zombieWorkerIdleSeconds;


    /**
     * @param string $channelId     Channel's unique identifier.
     * @param integer $defaultMaxWorkers
     * @param integer $defaultMaxLoad
     * @param Redis $redis
     * @param EntityManager $entityManager
     * @param Logger $logger
     */
    public function __construct(
        $channelId,
        $defaultMaxWorkers,
        $defaultMaxLoad,
        CelltrakRedis $redis,
        EntityManager $entityManager,
        Logger $logger
    ) {
        $this->channelId                = $channelId;
        $this->defaultMaxWorkers        = $defaultMaxWorkers;
        $this->defaultMaxLoad           = $defaultMaxLoad;
        $this->redis                    = $redis;
        $this->entityManager            = $entityManager;
        $this->logger                   = $logger;
        $this->zombieWorkerIdleSeconds  = self::DEFAULT_ZOMBIE_WORKER_IDLE_SECONDS;
    }

    /**
     * Attemps to acquire channel's control lock, which is required to start
     * and stop the channel.
     *
     * @return string|null  Returns null if lock could not be acquired.
     */
    public function acquireControlLock()
    {
        $controlLockKey = $this->getControlLockKey();
        $controlLockId = Util::guid();

        $options = [
            'NX',
            'EX' => self::CONTROL_LOCK_TTL
        ];

        $result = $this->redis->set($controlLockKey, $controlLockId, $options);
        return $result ? $controlLockId : null;
    }

    /**
     * Releases ownership of channel's control lock.
     *
     * @param string $controlLockId
     * @return boolean
     */
    public function releaseControlLock($controlLockId)
    {
        if ($this->hasControlLock($controlLockId)) {
            $controlLockKey = $this->getControlLockKey();
            $this->redis->del($controlLockKey);
            return true;
        } else {
            return false;
        }
    }

    /**
     * Starts the channel.
     *
     * @param string $controlLockId     Process must have acquired this channel's
     *                                  control lock in order to start.
     * @param string $error             Will be populated on failure.
     * @return boolean
     */
    public function start($controlLockId, &$error = null)
    {
        $this->logger->startThread();
        $this->logger->debug("EventQueueChannel: starting '{$this->channelId}' channel");

        if (!$this->hasControlLock($controlLockId)) {
            $this->logger->debug("EventQueueChannel: cannot start bc process doesn't own channel's control lock");
            $error = 'Process does not own channel\'s control lock';
            return false;
        }

        $status = $this->getStatus();

        if ($status && $status != self::STATUS_STOPPED) {
            $this->logger->debug("EventQueueChannel: cannot start bc channel is not stopped");
            $error = 'Channel not stopped';
            return false;
        }

        // Initialize channel hash. Setting status to PRE_START, which blocks
        // the dispatching of new events. This is done so  we can safely query
        // for the MAX queue_id for this channel that needs to be re-dispatched.
        // This start sequence is responsible for re-dispatching these events
        // properly so that when the channel finally flips to RUNNING status,
        // the workers will operate on the events in the correct order. This is
        // essential for pinned events.
        $stateKey = $this->getStateKey();

        $stateValues = [
            self::STATE_STATUS              => self::STATUS_PRE_START,
            self::STATE_PENDING_EVENT_COUNT => 0
        ];

        $this
            ->redis
            ->multi()
                ->hMSet($stateKey, $stateValues)
                // Use HSETNX for the max load and max worker fields because
                // we don't want to overwrite when a channel has had either of
                // these values overridden.
                ->hSetNx(
                    $stateKey,
                    self::STATE_MAX_LOAD_COUNT,
                    $this->defaultMaxLoad
                )
                ->hSetNx(
                    $stateKey,
                    self::STATE_MAX_WORKER_COUNT,
                    $this->defaultMaxWorkers
                )
            ->exec();

        $maxQueueId =
            $this
            ->entityManager
            ->getRepository('CelltrakEventQueueBundle:EventQueue')
            ->findChannelMaxQueueIdRequiringReDispatch($this->channelId);

        $this->logger->debug("EventQueueChannel: maxQueueId = {$maxQueueId}");

        if ($maxQueueId) {
            // Change to status to STARTING. This allows dispatching to continue
            // normally, but still prevents workers from running while peristed
            // events are re-dispatched.
            $this->redis->hSet($stateKey, self::STATE_STATUS, self::STATUS_STARTING);

            try {
                $this->reDispatchPersistedEvents($maxQueueId);
            } catch (\Exception $e) {
                $this->logger->error((string) $e);

                $error = "Failed with exception: {$e}";
                $this->stop($controlLockId);
                return;
            }
        }

        // "Flip the switch" so this channel is fully operational.
        $qualifiedChannelId = $this->getQualifiedChannelId();

        $stateValues = [
            self::STATE_STATUS          => self::STATUS_RUNNING,
            self::STATE_RUNNING_SINCE   => time()
        ];

        $this
            ->redis
            ->multi()
                ->hMSet($stateKey, $stateValues)
                // Notify worker monitor that this site's channel is active.
                ->sAdd(self::GLOBAL_ACTIVE_CHANNELS_KEY, $qualifiedChannelId)
            ->exec();

        $this->logger->debug("EventQueueChannel: '{$this->channelId}' channel is started");
        $this->logger->stopThread();
        return true;
    }

    /**
     * Stops the channel.
     *
     * @param string $controlLockId     Process must have acquired this channel's
     *                                  control lock in order to start.
     * @param string $error             Will be populated on failure.
     * @return boolean
     */
    public function stop($controlLockId, &$error = null)
    {
        $this->logger->startThread();
        $this->logger->debug("EventQueueChannel: stopping '{$this->channelId}' channel");

        if (!$this->hasControlLock($controlLockId)) {
            $this->logger->debug("EventQueueChannel: cannot stop bc process doesn't own channel's control lock");
            $error = 'Process does not own channel\'s control lock';
            return false;
        }

        if ($this->getStatus() == self::STATUS_STOPPED) {
            $this->logger->debug("EventQueueChannel: channel is already stopped");
            $error = 'Channel already stopped';
            return false;
        }

        $stateValues = [
            self::STATE_STATUS              => self::STATUS_STOPPING,
            self::STATE_PENDING_EVENT_COUNT => 0,
            self::STATE_RUNNING_SINCE       => -1
        ];

        $stateKey           = $this->getStateKey();
        $qualifiedChannelId = $this->getQualifiedChannelId();

        $this
            ->redis
            ->multi()
                // Change status to STOPPING to cease all activity in this channel.
                ->hMSet($stateKey, $stateValues)
                // Remove this site's channel from worker monitor's active list.
                ->sRem(self::GLOBAL_ACTIVE_CHANNELS_KEY, $qualifiedChannelId)
            ->exec();

        // Garbage collect most channel keys.
        // *IMPORTANT* Keep state and control lock keys otherwise this operation
        // will break.
        $keepKeys = [
            $this->getStateKey(),
            $this->getControlLockKey(),
            $this->getWorkersKey()
        ];
        $this->flushKeys($keepKeys);

        // Wait for all active workers to deactivate.
        while ($activeWorkerCount = $this->getActiveWorkerCount()) {
            $this->logger->debug("EventQueueChannel: waiting for {$activeWorkerCount} worker(s) to deactivate");
            // Give it another second before checking again.
            sleep(1);
        }

        // Change status to STOPPED. This allows the channel to be started again.
        $this->redis->hSet($stateKey, self::STATE_STATUS, self::STATUS_STOPPED);

        $this->logger->debug("EventQueueChannel: '{$this->channelId}' channel is stopped");
        $this->logger->stopThread();
        return true;
    }

    /**
     * Restarts channel.
     *
     * @param string $controlLockId     Process must have acquired this channel's
     *                                  control lock in order to start.
     * @param string $error             Will be populated on failure.
     * @return boolean
     */
    public function restart($controlLockId, &$error=null)
    {
        return $this->stop($controlLockId, $error)
            && $this->start($controlLockId, $error);
    }

    /**
     * Returns the operational metrics for this channel.
     *
     * @return array
     */
    public function inspect()
    {
        $workerCount = $this->getActiveWorkerCount();

        $state = $this->redis->hGetAll($this->getStateKey());

        if (isset($state[self::STATE_STATUS])) {
            $status = $state[self::STATE_STATUS];
        } else {
            $status = 'N/A';
        }

        if (isset($state[self::STATE_PENDING_EVENT_COUNT])) {
            $pendingEventCount = $state[self::STATE_PENDING_EVENT_COUNT];
        } else {
            $pendingEventCount = 'N/A';
        }

        if (isset($state[self::STATE_RUNNING_SINCE])) {
            $runningSince = $state[self::STATE_RUNNING_SINCE];

            if ($runningSince == -1) {
                $runningSince = 'N/A';
            } else {
                $runningSince = date('Y-m-d H:i:s', (int) $runningSince);
            }
        } else {
            $runningSince = 'N/A';
        }

        if (isset($state[self::STATE_MAX_LOAD_COUNT])) {
            $maxLoadCount = $state[self::STATE_MAX_LOAD_COUNT];
        } else {
            $maxLoadCount = 'N/A';
        }

        if (isset($state[self::STATE_MAX_WORKER_COUNT])) {
            $maxWorkerCount = $state[self::STATE_MAX_WORKER_COUNT];
        } else {
            $maxWorkerCount = 'N/A';
        }

        return [
            'workerCount'       => $workerCount,
            'status'            => $status,
            'pendingEventCount' => $pendingEventCount,
            'runningSince'      => $runningSince,
            'maxLoadCount'      => $maxLoadCount,
            'maxWorkerCount'    => $maxWorkerCount
        ];
    }

    /**
     * Returns channel's status.
     * @return string EventQueueChannel::STATUS_*
     */
    public function getStatus()
    {
        $stateKey = $this->getStateKey();
        return $this->redis->hGet($stateKey, self::STATE_STATUS);
    }

    /**
     * Indicates whether the channel is running.
     * @return boolean
     */
    public function isRunning()
    {
        $stateKey = $this->getStateKey();
        $status = $this->redis->hGet($stateKey, self::STATE_STATUS);
        return $status == self::STATUS_RUNNING;
    }

    /**
     * Logs that a worker has been newly activated.
     * @param  integer $workerId
     * @param  string $hostname  Host running worker process.
     * @param  integer $pid      System process id for worker process.
     * @return void
     */
    public function activateWorker($workerId, $hostname, $pid)
    {
        $this->logger->debug("EventQueueChannel: activate workerId {$workerId} running on '{$hostname}' as pid {$pid}");

        $worker =
            $this
            ->entityManager
            ->getRepository('CelltrakEventQueueBundle:EventQueueWorker')
            ->_mustFind($workerId);

        if ($worker->getChannel() != $this->channelId) {
            throw new \RuntimeException("Cannot activate {$worker} because it's not in '{$this->channelId}' channel");
        }

        if ($worker->getStatus() != WorkerEntity::STATUS_PROVISIONING) {
            throw new \RuntimeException("Cannot activate {$worker} because it does not have provisioning status");
        }

        // Check if channel is RUNNING and if so, add worker to this channel's
        // worker set.
        $script = "
            -- Ensure channel is running.

            if redis.call('HGET', KEYS[1], '" . self::STATE_STATUS . "') ~= '" . self::STATUS_RUNNING . "' then
                return -1
            end

            -- Add worker heartbeat so channel is aware of it.

            redis.call('ZADD', KEYS[2], ARGV[2], ARGV[1])
            return 1";

        // The Redis keys used by this script.
        $keys = [
            $this->getStateKey(),
            $this->getWorkersKey()
        ];

        // The additional arguments used by this script.
        $args = [
            $workerId,
            time()
        ];

        $result = $this->redis->runScript($script, $keys, $args);

        $this->logger->debug("EventQueueChannel: activateWorker Lua script result '{$result}'");

        if ($result == -1) {
            // Channel is not running. Cannot activate worker.
            return false;
        }

        // Record activation on worker's database record.
        $updateFields = [
            'status'        => WorkerEntity::STATUS_ACTIVE,
            'activatedOn'   => time(),
            'hostName'      => $hostname,
            'pid'           => $pid
        ];
        $this->entityManager->updateForFields($worker, $updateFields);
        return true;
    }

    /**
     * Deaactivates worker.
     * @param  integer $workerId
     * @param  string $deactivatedReason
     * @param  string $error
     * @return boolean
     */
    public function deactivateWorker(
        $workerId,
        $deactivatedReason,
        $error = null
    ) {
        $this->logger->debug("EventQueueChannel: deactivate workerId {$workerId} with reason '{$deactivatedReason}', error = '{$error}'");

        $worker =
            $this
            ->entityManager
            ->getRepository('CelltrakEventQueueBundle:EventQueueWorker')
            ->_find($workerId);

        if (!$worker) {
            return false;
        }

        if ($worker->getChannel() != $this->channelId) {
            throw new \RuntimeException("Cannot deactivate {$worker} because it's not in '{$this->channelId}' channel");
        }

        if ($worker->getStatus() == WorkerEntity::STATUS_DEACTIVATED) {
            return false;
        }

        // Remove worker from channel's sorted set so it no longer is considered
        // active. At the same time, remove worker kill switch since most of the
        // time that's what will trigger deactivation.
        $workersKey = $this->getWorkersKey();
        $killSwitchesKey = $this->getWorkerKillSwitchesKey();

        $this
            ->redis
            ->multi(\Redis::PIPELINE)
                ->zRem($workersKey, $workerId)
                ->sRem($killSwitchesKey, $workerId)
            ->exec();

        // Release pin key if it has acquired one.
        // NOTE: In case of stopping the channel, the pin keys worker locks key
        // will have been deleted.
        $pinKeysWorkerLocksKey = $this->getPinKeysWorkerLocksKey();
        $pinKeysWorkerLocks = $this->redis->hGetAll($pinKeysWorkerLocksKey);

        if ($pinKeysWorkerLocks &&
            ($assignedPinKey = array_search($workerId, $pinKeysWorkerLocks))) {
            $this->releasePinKeyLock($assignedPinKey, $workerId);
        }

        // Log deactivation with database.
        $updateFields = [
            'status'            => WorkerEntity::STATUS_DEACTIVATED,
            'deactivatedOn'     => time(),
            'deactivatedReason' => $deactivatedReason,
            'exception'         => $error
        ];
        $this->entityManager->updateForFields($worker, $updateFields);
        return true;
    }

    /**
     * Kills specified worker by setting their kill switch.
     *
     * NOTE: This method is intended for external processes (namely the worker
     * monitor via the EventQueueWorkerController) to trigger the graceful
     * deactivation of a worker. The worker will handle all the necessary
     * cleanup itself after learning of its demise.
     *
     * @param integer $workerId
     * @return boolean  Indicates whether worker kill switch set successfully.
     */
    public function killWorker($workerId)
    {
        $this->logger->debug("EventQueueChannel: kill worker {$workerId}");

        $worker =
            $this
            ->entityManager
            ->getRepository('CelltrakEventQueueBundle:EventQueueWorker')
            ->_find($workerId);

        if (!$worker) {
            return false;
        }

        if ($worker->getChannel() != $this->channelId) {
            throw new \RuntimeException("Cannot kill {$worker} because it's not in '{$this->channelId}' channel");
        }

        if ($worker->getStatus() != WorkerEntity::STATUS_ACTIVE) {
            return false;
        }

        // Set the kill switch.
        $killSwitchesKey = $this->getWorkerKillSwitchesKey();
        $this->redis->sAdd($killSwitchesKey, $workerId);
        return true;
    }

    /**
     * Kills all workers.
     *
     * @param string $controlLockId     Process must have acquired this channel's
     *                                  control lock in order to start.
     * @param string $error             Will be populated on failure.
     * @return integer|boolean          Returns the number of killed workers.
     *                                  Returns false on failure.
     */
    public function killAllWorkers($controlLockId, $error = null)
    {
        $this->logger->startThread();
        $this->logger->debug("EventQueueChannel: killing all workers for '{$this->channelId}' channel");

        if (!$this->hasControlLock($controlLockId)) {
            $this->logger->debug("EventQueueChannel: cannot kill all workers bc process doesn't own channel's control lock");
            $error = 'Process does not own channel\'s control lock';
            return false;
        }

        if ($this->getStatus() != self::STATUS_RUNNING) {
            $this->logger->debug("EventQueueChannel: channel is not running");
            $error = 'Channel is not running';
            return false;
        }

        // Retrieve active worker IDs.
        $workersKey = $this->getWorkersKey();
        $workerIds = $this->redis->zRangeByScore($workersKey, '-inf', '+inf');

        if (!$workerIds) {
            return 0;
        }

        // Set kill switches for all workers.
        $killSwitchesKey = $this->getWorkerKillSwitchesKey();
        $this->redis->sAdd($killSwitchesKey, ...$workerIds);
        return count($workerIds);
    }

    /**
     * Indicates whether worker is active for this channel.
     * @param  integer  $workerId
     * @return boolean
     */
    public function hasWorker($workerId)
    {
        $workersKey = $this->getWorkersKey();
        $zombieTime = time() - $this->zombieWorkerIdleSeconds;
        $lastCheckIn = $this->redis->zScore($workersKey, $workerId);

        if ($lastCheckIn && $lastCheckIn > $zombieTime) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * Returns the number of active workers for this channel.
     *
     * @return integer
     */
    public function getActiveWorkerCount()
    {
        $workersKey = $this->getWorkersKey();
        $zombieTime = time() - $this->zombieWorkerIdleSeconds;
        return $this->redis->zCount($workersKey, '(' . $zombieTime, '+inf');
    }

    /**
     * Notify channel that worker process is still running and find out if
     * worker has been killed.
     * @param  integer $workerId
     * @return boolean        Returns false if worker has been killed.
     */
    public function checkInWorker($workerId)
    {
        // The check-in logic requires two steps:
        //  1. Ensure that the worker hasn't been killed.
        //  2. If so, increment the worker's check-in timestamp (heartbeat).
        //
        // The worker will have its kill switch set when it's been killed
        // by the worker monitor.

        $script = "
            -- Ensure channel is running.

            if redis.call('HGET', KEYS[1], '" . self::STATE_STATUS . "') ~= '" . self::STATUS_RUNNING . "' then
                return -1
            end

            -- Ensure worker's kill switch isn't set.

            if redis.call('SISMEMBER', KEYS[2], ARGV[1]) == 1 then
                return -1
            end

            -- Update worker's heartbeat.

            redis.call('ZADD', KEYS[3], ARGV[2], ARGV[1])

            return 1";

        // The Redis keys used by this script.
        $keys = [
            $this->getStateKey(),
            $this->getWorkerKillSwitchesKey(),
            $this->getWorkersKey()
        ];

        // The additional arguments used by this script.
        $args = [
            $workerId, // the worker
            time() // check-in time
        ];

        $result = $this->redis->runScript($script, $keys, $args);
        return $result == 1 ? true : false;
    }

    /**
     * Adds event into channel's queue.
     * @param string $event
     */
    public function addEvent($event)
    {
        // Adding a standard (non-pinned) event just requires adding it to the
        // end of the channel's queue and incrementing channel's pending event
        // count. We have to maintain pending event count rather than rely on
        // size of queue to support pinned events (see addPinnedEvent).

        $script = "
            local channelStatus = redis.call('HGET', KEYS[1], '" . self::STATE_STATUS . "')

            -- Can't add event and can't retry if channel is stopped or is in
            -- process of stopping.

            if channelStatus == false
                or channelStatus == '" . self::STATUS_STOPPING . "'
                or channelStatus == '" . self::STATUS_STOPPED . "' then
                return {false, false}
            end

            -- Can't add event but should immediately retry if channel is in
            -- pre-start phase.

            if channelStatus == '" . self::STATUS_PRE_START . "' then
                return {false, true}
            end

            -- Add event to end of queue and increment pending event count.

            redis.call('RPUSH', KEYS[2], ARGV[1])
            redis.call('HINCRBY', KEYS[1], '" . self::STATE_PENDING_EVENT_COUNT . "', 1)
            return {true, false}";

        $keys = [
            $this->getStateKey(),
            $this->getEventsKey(),
        ];

        $args = [
            $event
        ];

        $result = $this->redis->runScript($script, $keys, $args);

        list($added, $retry) = $result;

        if ($added) {
            return true;
        } elseif (!$retry) {
            return false;
        } else {
            // Channel has status set to PRE_START, which temporarily blocks
            // event dispatching.
            return $this->addEvent($event);
        }
    }

    /**
     * Adds pinned event into channel's queue.
     * @param string $event
     * @param string $pinKey
     */
    public function addPinnedEvent($event, $pinKey)
    {
        // Adding a pinned event is more complicated than adding standard events.
        // To notify the workers that there is a pinned event, we still need to
        // add something into the channel's main queue. But instead of adding
        // the event itself, we add a pointer to the pin key. The event itself
        // is added to the pin key's dedicated queue. When a worker receives the
        // pointer from the main queue, it knows to process events from the
        // pinned queue. This scheme gurantees that:
        //
        //      1. Pinnned events are processed in a first come, first serve
        //      order just like standard events
        //      2. Events for the same pin key are processed in series

        $script = "
            local channelStatus = redis.call('HGET', KEYS[1], '" . self::STATE_STATUS . "')

            -- Can't add event and can't retry if channel is stopped or is in
            -- process of stopping.

            if channelStatus == false
                or channelStatus == '" . self::STATUS_STOPPING . "'
                or channelStatus == '" . self::STATUS_STOPPED . "' then
                return {false, false}
            end

            -- Can't add event but should immediately retry if channel is in
            -- pre-start phase.

            if channelStatus == '" . self::STATUS_PRE_START . "' then
                return {false, true}
            end

            -- Can't add event but should immediately retry if dispatch lock set
            -- for pin key.

            if redis.call('HGET', KEYS[2], ARGV[1]) == '" . self::PIN_KEY_CONTROL_LOCK_DISPATCH . "' then
                return {false, true}
            end

            -- Add event to end of pin key's queue and add pin key pointer to
            -- end of channel's event queue. Also increment pending event count.

            redis.call('RPUSH', KEYS[3], ARGV[2])
            redis.call('RPUSH', KEYS[4], ARGV[3])
            redis.call('HINCRBY', KEYS[1], '" . self::STATE_PENDING_EVENT_COUNT . "', 1)
            return {true, false}";

        $keys = [
            $this->getStateKey(),
            $this->getPinKeysControlLocksKey(),
            $this->getEventsKey(),
            $this->getPinnedEventsKey($pinKey)
        ];

        $args = [
            $pinKey,
            $this->getPinKeyPointer($pinKey),
            $event
        ];

        $result = $this->redis->runScript($script, $keys, $args);

        list($added, $retry) = $result;

        if ($added) {
            return true;
        } elseif (!$retry) {
            return false;
        } else {
            // Channel has status set to PRE_START, which temporarily blocks
            // event dispatching.
            return $this->addPinnedEvent($event, $pinKey);
        }
    }

    /**
     * Returns next entry in the channel's event queue.
     *
     * The event queue contains two types of entries: standard events and pin
     * key pointers. This method normalizes those two types into a tuple. The
     * first element is a boolean indicating whether it's a pin key. The second
     * element will either be the pin key string or the standard event returned
     * as a stdClass.
     *
     * @return array [$isPinKeyPointer, $data]
     */
    public function getNextQueueEntry($workerId)
    {
        $script = "
            -- Ensure that channel is running.

            if redis.call('HGET', KEYS[1], '" . self::STATE_STATUS . "') ~= '" . self::STATUS_RUNNING . "' then
                return -1
            end

            -- Ensure that worker kill switch isn't set.

            if redis.call('SISMEMBER', KEYS[2], ARGV[1]) == 1 then
                return -1
            end

            -- Update worker heartbeat.

            redis.call('ZADD', KEYS[3], ARGV[2], ARGV[1])

            -- Get next entry in queue (will be event or pin pointer).

            local entry = redis.call('LPOP', KEYS[4])

            -- Reduce pending event count if entry returned and is not pointer.

            if entry and
                entry:sub(1, " . strlen(self::PIN_KEY_POINTER_PREFIX) . ") ~= '" . self::PIN_KEY_POINTER_PREFIX . "' then

                if redis.call('HINCRBY', KEYS[1], '" . self::STATE_PENDING_EVENT_COUNT . "', -1) < 0 then
                    redis.call('HSET', KEYS[1], '" . self::STATE_PENDING_EVENT_COUNT . "', 0)
                end
            end

            return entry";

        // The Redis keys used by this script.
        $keys = [
            $this->getStateKey(),
            $this->getWorkerKillSwitchesKey(),
            $this->getWorkersKey(),
            $this->getEventsKey()
        ];

        // The additional arguments used by this script.
        $args = [
            $workerId,
            time()
        ];

        $result = $this->redis->runScript($script, $keys, $args);

        if ($result == -1) {
            // Worker needs to deactivate.
            return false;
        }

        if (!$result) {
            // Important to return [] and not false, which is what will be
            // returned when there isn't another event. False instructs the
            // worker to deactivate while [] simply tells it that there aren't
            // more events.
            return [];
        }

        if (strpos($result, self::PIN_KEY_POINTER_PREFIX) === 0) {
            // This is a pointer to a pinned event.
            $isPinKeyPointer = true;
            $data = substr($result, strlen(self::PIN_KEY_POINTER_PREFIX));
        } else {
            $isPinKeyPointer = false;
            $data = $result;
        }

        return [$isPinKeyPointer, $data];
    }

    /**
     * Returns next event in queue for specified pin key.
     *
     * @param string $pinKey
     * @return string
     */
    public function getNextPinnedEvent($pinKey, $workerId)
    {
        $script = "
            -- Ensure the channel is running.

            if redis.call('HGET', KEYS[1], '" . self::STATE_STATUS . "') ~= '" . self::STATUS_RUNNING . "' then
                return -1
            end

            -- Ensure that worker kill switch isn't set.

            if redis.call('SISMEMBER', KEYS[2], ARGV[1]) == 1 then
                return -1
            end

            -- Update worker heartbeat.

            redis.call('ZADD', KEYS[3], ARGV[2], ARGV[1])

            -- Prevent retrieval of next event for this pin key if its control
            -- lock is set.

            if redis.call('HEXISTS', KEYS[4], ARGV[3]) == 1 then
                return nil
            end

            local event = redis.call('LPOP', KEYS[5])

            -- If event retrieved, reduce pending event count.

            if event then
                if redis.call('HINCRBY', KEYS[1], '" . self::STATE_PENDING_EVENT_COUNT . "', -1) < 0 then
                    redis.call('HSET', KEYS[1], '" . self::STATE_PENDING_EVENT_COUNT . "', 0)
                end
            end

            return event";

        // The Redis keys used by this script.
        $keys = [
            $this->getStateKey(),
            $this->getWorkerKillSwitchesKey(),
            $this->getWorkersKey(),
            $this->getPinKeysControlLocksKey(),
            $this->getPinnedEventsKey($pinKey)
        ];

        // The additional arguments used by this script.
        $args = [
            $workerId,
            time(),
            $pinKey
        ];

        $result = $this->redis->runScript($script, $keys, $args);

        if ($result == -1) {
            // Worker needs to deactivate.
            return false;
        }

        if (!$result) {
            // Important to return null and not false, which is what will be
            // returned when there isn't another event. False instructs the
            // worker to deactivate while null simply tells it that there aren't
            // more events.
            return null;
        }

        return $result;
    }

    /**
     * Used by worker to acquire ownership lock for specified pin key.
     *
     * Pinned events MUST be processed in series so it's essential that only one
     * worker is responsible for handling the events tied to the same pin key.
     *
     * @param string $pinKey
     * @param integer $workerId
     * @return boolean
     */
    public function acquirePinKeyLock($pinKey, $workerId)
    {
        $this->logger->debug("EventQueueChannel: acquire pin key '{$pinKey}' lock for workerId {$workerId}");

        $script = "
            -- Acquire lock when:
            -- (1) Pin key's control lock not set to prevent work
            -- (2) Pin key's event queue has at least 1 event
            -- (3) Pin key's lock not already owned by a different worker

            if redis.call('HGET', KEYS[1], ARGV[1]) ~= '" . self::PIN_KEY_CONTROL_LOCK_WORK . "' and
                redis.call('LLEN', KEYS[2]) > 0 and
                redis.call('HSETNX', KEYS[3], ARGV[1], ARGV[2]) == 1 then
                return 1
            else
                return -1
            end";

        // The Redis keys used by this script.
        $keys = [
            $this->getPinKeysControlLocksKey(),
            $this->getPinnedEventsKey($pinKey),
            $this->getPinKeysWorkerLocksKey()
        ];

        // The additional arguments used by this script.
        $args = [
            $pinKey,
            $workerId
        ];

        $result = $this->redis->runScript($script, $keys, $args);
        return $result === 1 ? true : false;
    }

    /**
     * Releases pin key lock owned by worker.
     *
     * @param string $pinKey
     * @param integer $workerId
     * @return boolean
     */
    public function releasePinKeyLock($pinKey, $workerId)
    {
        $this->logger->debug("EventQueueChannel: release pin key '{$pinKey}' lock from workerId {$workerId}");

        $script = "
            -- Ensure pin key's lock is owned by this worker.

            if redis.call('HGET', KEYS[1], ARGV[1]) ~= ARGV[2] then
                return -1
            end

            -- Remove lock.

            redis.call('HDEL', KEYS[1], ARGV[1])

            -- Add pin key pointer back into channel's main event queue if there
            -- is at least 1 event in pin key's queue. This prevents potential
            -- race condition where pinned event comes in right after assigned
            -- worker checks and simulatenously, other worker cleared out the
            -- corresponding pin key pointer from the main queue.

            if redis.call('LLEN', KEYS[2]) > 0 then
                redis.call('LPUSH', KEYS[3], ARGV[3])
            end

            return 1";

        // The Redis keys used by this script.
        $keys = [
            $this->getPinKeysWorkerLocksKey(),
            $this->getPinnedEventsKey($pinKey),
            $this->getEventsKey()
        ];

        // The additional arguments used by this script.
        $args = [
            $pinKey,
            $workerId,
            $this->getPinKeyPointer($pinKey)
        ];

        $result = $this->redis->runScript($script, $keys, $args);
        return $result === 1 ? true : false;
    }

    /**
     * Retries failed persisted event by re-dispatching. In the case of a pinned
     * event, this will retry this event along with all subsequent events for its
     * pin key.
     *
     * @param EventQueue $queueEntry
     * @return ingeger  Returns the number of events successfully re-dispatched.
     */
    public function retryFailedPersistedEvent(EventQueue $queueEntry)
    {
        $this->logger->debug("EventQueueChannel: retrying failed {$queueEntry}");

        if ($queueEntry->getStatus() != EventQueue::STATUS_EXCEPTION_OCCURRED) {
            throw new \InvalidArgumentException("{$queueEntry} does not have exception status");
        }

        $pinKey = $queueEntry->getPinKey();

        if ($pinKey) {
            $reDispatchedCount = $this->retryPinnedPersistedEvents($pinKey);
        } else {
            $reDispatchedCount = $this->retryPersistedEvent($queueEntry);
        }

        return $reDispatchedCount;
    }

    /**
     * Sets max number of pending events per worker for this channel.
     *
     * @param integer $maxLoadCount
     * @return void
     */
    public function setMaxLoadCount($maxLoadCount)
    {
        if (!is_int($maxLoadCount) || $maxLoadCount <= 0) {
            throw new \InvalidArgumentException('$maxLoadCount must be int greater than 0');
        }

        $stateKey = $this->getStateKey();
        $this->redis->hSet($stateKey, self::STATE_MAX_LOAD_COUNT, $maxLoadCount);
    }

    /**
     * Sets max number of workers allowed for this channel.
     *
     * @param integer $maxWorkerCount
     * @return void
     */
    public function setMaxWorkerCount($maxWorkerCount)
    {
        if (!is_int($maxWorkerCount) || $maxWorkerCount <= 0) {
            throw new \InvalidArgumentException('$maxWorkerCount must be int greater than 0');
        }

        $stateKey = $this->getStateKey();
        $this->redis->hSet($stateKey, self::STATE_MAX_WORKER_COUNT, $maxWorkerCount);
    }

    /**
     * Sets $tenantKey.
     * @param string $tenantKey
     * @return void
     */
    public function setTenantKey($tenantKey)
    {
        $this->tenantKey = $tenantKey;
    }

    /**
     * Returns $tenantKey
     * @return string
     */
    public function getTenantKey()
    {
        return $this->tenantKey;
    }

    /**
     * Sets $zombieIdleWorkerSeconds.
     * @param integer $zombieIdleWorkerSeconds
     */
    public function setZombieWorkerIdleSeconds($zombieWorkerIdleSeconds)
    {
        if (!is_int($zombieWorkerIdleSeconds)) {
            throw new \InvalidArgumentException('$zombieWorkerIdleSeconds must be an int');
        }

        $this->zombieWorkerIdleSeconds = $zombieWorkerIdleSeconds;
    }

    /**
     * Returns channel's identifier.
     * @return string
     */
    public function getChannelId()
    {
        return $this->channelId;
    }

    /**
     * Returns channel's qualified identifier (includes Site ID).
     * @return string
     */
    public function getQualifiedChannelId()
    {
        if ($this->tenantKey) {
            $qualifiedChannelId = $this->tenantKey
                                . self::QUALIFIED_CHANNEL_ID_DELIM
                                . $this->channelId;
        } else {
            $qualifiedChannelId = $this->channelId;
        }

        return $qualifiedChannelId;
    }

    /**
     * Re-dispatch events persisted in the database queue that have not been
     * processed.
     *
     * @param integer $maxQueueId
     * @return integer  Returns number of re-dispatched events.
     */
    protected function reDispatchPersistedEvents($maxQueueId)
    {
        $queueRepo = $this->entityManager->getRepository('CelltrakEventQueueBundle:EventQueue');

        $queueRepo
            ->flagChannelReDispatchEventsAsPending($this->channelId, $maxQueueId);

        $reDispatchedCount = 0;
        $batchSize = self::RE_DISPATCH_EVENT_BATCH_SIZE;

        do {
            $queueEntries = $queueRepo->_findChannelEventsRequiringReDispatch(
                $this->channelId,
                $maxQueueId,
                null, // No pin key.
                $batchSize
            );

            foreach ($queueEntries as $queueEntry) {
                $event = json_encode($queueEntry);
                $pinKey = $queueEntry->getPinKey();

                try {
                    if ($pinKey) {
                        $this->prependPinnedEvent($event, $pinKey);
                    } else {
                        $this->prependEvent($event);
                    }

                    $reDispatchedCount += 1;
                } catch (\Exception $e) {
                    $this->logger->error((string) $e);
                    $this->logReDispatchFailure($queueEntry);
                }

                // Keep lowering the maxQueueId so query batches properly.
                // Subtract 1 b/c find query is inclusive.
                $maxQueueId = $queueEntry->getQueueId() - 1;
            }
        } while (count($queueEntries) == $batchSize);

        return $reDispatchedCount;
    }

    /**
     * Retries standard persisted event.
     *
     * @param EventQueue $queueEntry
     * @return integer  Returns 1 if successfully re-dispatched; 0 if not.
     */
    protected function retryPersistedEvent(EventQueue $queueEntry)
    {
        $this->logger->debug("EventQueueChannel: retrying persisted {$queueEntry}");

        $updateFields = [
            'status' => EventQueue::STATUS_PENDING,
            'workerId' => null
        ];

        try {
            $this->entityManager->updateForFields($queueEntry, $updateFields);
        } catch (\Exception $e) {
            // No need to throw this exception because worker will update
            // this queue entry's status.
        }

        // Retrieve finished listener ids for this queue entry.
        $listenerIds = $this->getFinishedListenerIds($queueEntry);
        $listenerIds = join(',', $listenerIds);
        $queueEntry->setFinishedListenerIds($listenerIds);

        $event = json_encode($queueEntry);

        try {
            $this->prependEvent($event);
            return 1;
        } catch (\Exception $e) {
            $this->logger->error((string) $e);
            $this->logReDispatchFailure($queueEntry);
            return 0;
        }
    }

    /**
     * Retries pinned events for pinKey.
     *
     * @param string $pinKey
     * @return integer  Returns number of events successfully re-dispatched.
     */
    protected function retryPinnedPersistedEvents($pinKey)
    {
        $this->logger->debug("EventQueueChannel: retrying persisted events for pin key '{$pinKey}'");

        // Attempt to acquire pin key's control lock and set it to temporarily
        // prevent dispatching of new events for this pin key. This temporary
        // embargo makes it possible to safely query for the max queue entry
        // that needs to be retried for this pin key without concern for a race
        // condition with new events getting dispatched. Setting the pin key
        // lock also prevents workers from fetching any more of this pin key's
        // events from the queue.
        $pinKeyControlLocksKey = $this->getPinKeysControlLocksKey();

        $result =
            $this
            ->redis
            ->hSetNx(
                $pinKeyControlLocksKey,
                $pinKey,
                self::PIN_KEY_CONTROL_LOCK_DISPATCH
            );

        if (!$result) {
            $this->logger->debug("EventQueueChannel: pin key's control lock acquired by different process");
            return 0;
        }

        // Delete pinned events stack before rebuilding from persisted queue
        // entries.
        $pinnedEventsKey = $this->getPinnedEventsKey($pinKey);
        $this->redis->del($pinnedEventsKey);

        $pinKeysWorkerLocksKey = $this->getPinKeysWorkerLocksKey();

        // Wait until worker releases pin key before proceeding.
        // TODO: Add timeout here.
        while ($this->redis->hExists($pinKeysWorkerLocksKey, $pinKey)) {
            sleep(1);
        }

        $queueRepo = $this->entityManager
            ->getRepository('CelltrakEventQueueBundle:EventQueue');

        $maxQueueId =
            $queueRepo
            ->findPinKeyMaxQueueIdForRetry($this->channelId, $pinKey);

        if (!$maxQueueId) {
            $this->redis->hDel($pinKeyControlLocksKey, $pinKey);
            return 0;
        }

        // Change control lock to WORK so that new events resume dispatching
        // for this pin key but workers still can't operate on it.
        $this
        ->redis
        ->hSet($pinKeyControlLocksKey, $pinKey, self::PIN_KEY_CONTROL_LOCK_WORK);

        $queueRepo
        ->flagPinKeyRetryEventsAsPending($this->channelId, $pinKey, $maxQueueId);

        $queueEntries =
            $queueRepo
            ->_findChannelEventsRequiringReDispatch(
                $this->channelId,
                $maxQueueId,
                $pinKey
            );

        $hasReDispatchFailure = false;
        $reDispatchedCount = 0;

        foreach ($queueEntries as $queueEntry) {
            if ($hasReDispatchFailure) {
                // Cannot
                $this->logReDispatchFailure($queueEntry);
                continue;
            }

            $event = json_encode($queueEntry);

            try {
                $this->prependPinnedEvent($event, $pinKey);
                $reDispatchedCount += 1;
            } catch (\Exception $e) {
                $this->logger->error((string) $e);
                $this->logReDispatchFailure($queueEntry);
                $hasReDispatchFailure = true;
            }
        }

        if ($hasReDispatchFailure) {
            // Remove the control lock and re-delete the pinned events stack.
            $this
            ->redis
            ->multi()
                ->hDel($pinKeyControlLocksKey, $pinKey)
                ->del($pinnedEventsKey)
            ->exec();

            return 0;
        }

        // Remove control lock so work can continue on this pin key and re-add
        // pin key pointer to main event stack so a worker knows to pick up this
        // pin key. Technically, the pin key pointer has been added with each
        // re-dispatched event. But workers receiving those pointers would have
        // been told to disregard them until now since the pin key's control
        // lock was still set.
        $eventsKey = $this->getEventsKey();
        $pinKeyPointer = $this->getPinKeyPointer($pinKey);

        $this
        ->redis
        ->multi()
            ->hDel($pinKeyControlLocksKey, $pinKey)
            ->lPush($eventsKey, $pinKeyPointer)
        ->exec();

        return $reDispatchedCount;
    }

    /**
     * Adds event to the *beginning* of the channel's queue.
     *
     * This is notably different than addEvent because it adds the event to the
     * front of the line. prependEvent is designed soley for re-dispatching
     * persisted events when the channel [re]starts.
     *
     * @param string $event
     * @return void
     */
    protected function prependEvent($event)
    {
        // Adding a standard (non-pinned) event just requires adding it to the
        // end of the channel's queue and incrementing channel's pending event
        // count. We have to maintain pending event count rather than rely on
        // size of queue to support pinned events (see addPinnedEvent).

        $script = "
            -- Ensure channel is not stopped or in the process of stopping.

            local channelStatus = redis.call('HGET', KEYS[1], '" . self::STATE_STATUS . "')

            if channelStatus == '" . self::STATUS_STOPPING . "' or
                channelStatus == '" . self::STATUS_STOPPED . "' then
                return -1
            end

            -- Add event to the beginning of the channel's queue and increment
            -- the pending event count.

            redis.call('LPUSH', KEYS[2], ARGV[1])
            redis.call('HINCRBY', KEYS[1], '" . self::STATE_PENDING_EVENT_COUNT . "', 1)
            return 1";

        $keys = [
            $this->getStateKey(),
            $this->getEventsKey()
        ];

        $args = [
            $event
        ];

        $result = $this->redis->runScript($script, $keys, $args);

        if ($result == -1) {
            throw new \RuntimeException("'{$this->channelId}' channel does not have appropriate status for pre-pending event");
        }
    }

    /**
     * Adds pinned event to the *beginning* of the channel's queue.
     *
     * This is notably different than addEvent because it adds the event to the
     * front of the line. prependEvent is designed soley for re-dispatching
     * persisted events when the channel [re]starts.
     *
     * @param string $event
     * @param string $pinKey
     * @return void
     */
    protected function prependPinnedEvent($event, $pinKey)
    {
        // Adding a pinned event is more complicated than adding standard events.
        // To notify the workers that there is a pinned event, we still need to
        // add something into the channel's main queue. But instead of adding
        // the event itself, we add a pointer to the pin key. The event itself
        // is added to the pin key's dedicated queue. When a worker receives the
        // pointer from the main queue, it knows to process events from the
        // pinned queue. This scheme gurantees that:
        //
        //      1. Pinnned events are processed in a first come, first serve
        //      order just like standard events
        //      2. Events for the same pin key are processed in series

        $script = "
            -- Ensure the channel is not stopped or in the process of stopping.

            local channelStatus = redis.call('HGET', KEYS[1], '" . self::STATE_STATUS . "')

            if channelStatus == '" . self::STATUS_STOPPING . "' or
                channelStatus == '" . self::STATUS_STOPPED . "' then
                return -1
            end

            -- Add pin key pointer to the beginning of the channel's main queue
            -- and the pinned event to the beginning of the pin key's queue.
            -- Also increment the pending event count.

            redis.call('LPUSH', KEYS[2], ARGV[1])
            redis.call('LPUSH', KEYS[3], ARGV[2])
            redis.call('HINCRBY', KEYS[1], '" . self::STATE_PENDING_EVENT_COUNT . "', 1)
            return 1";

        $keys = [
            $this->getStateKey(),
            $this->getEventsKey(),
            $this->getPinnedEventsKey($pinKey)
        ];

        $args = [
            $this->getPinKeyPointer($pinKey),
            $event
        ];

        $result = $this->redis->runScript($script, $keys, $args);

        if ($result == -1) {
            throw new \RuntimeException("'{$this->channelId}' channel does not have appropriate status for pre-pending pinned event");
        }
    }

    /**
     * Logs queue entry re-dispatch failure.
     * @param EventQueue $queueEntry
     * @return void
     */
    protected function logReDispatchFailure(EventQueue $queueEntry)
    {
        $fields = ['status' => EventQueue::STATUS_NOT_DISPATCHED];
        try {
            $this->entityManager->updateForFields($queueEntry, $fields);
        } catch (\Exception $e) {
            // No need to throw this exception. Ideally the database record
            // would have the proper status, but worst case, this event will
            // appear pending for too long of a time. That is enough to
            // indicate a channel restart is required.
        }
    }

    /**
     * Returns queue entry's finished listener ids.
     * @param EventQueue $queueEntry
     * @return array
     */
    protected function getFinishedListenerIds(EventQueue $queueEntry)
    {
        $criteria = [
            'queueId' => $queueEntry->getQueueId(),
            'isFinished' => true
        ];

        $listenerLogs =
            $this
            ->entityManager
            ->getRepository('CelltrakEventQueueBundle:EventQueueListenerLog')
            ->_findBy($criteria);

        $listenerIds = [];

        foreach ($listenerLogs as $listenerLog) {
            $listenerIds[] = $listenerLog->getListenerId();
        }

        return $listenerIds;
    }

    /**
     * Indicates whether process owns channel's control lock.
     *
     * @param string $controlLockId
     * @return boolean
     */
    protected function hasControlLock($controlLockId)
    {
        $controlLockKey = $this->getControlLockKey();
        $currentLockId = $this->redis->get($controlLockKey);
        return $currentLockId === $controlLockId;
    }

    /**
     * Remove keys created for this channel.
     *
     * **IMPORTANT** This method deletes keys from Redis. If $keepKeys is
     * empty, it will remove all of the keys created for this channel.
     *
     * @param array $keepKeys
     * @return void
     */
    protected function flushKeys(array $keepKeys = [])
    {
        $iterator = null;
        $keyPattern = $this->qualifyKey('*');

        while ($keys = $this->redis->scan($iterator, $keyPattern)) {
            $keys = array_diff($keys, $keepKeys);
            $this->redis->del($keys);
        }
    }

    /**
     * Returns channel's control lock key.
     * @return string
     */
    protected function getControlLockKey()
    {
        return $this->qualifyKey('control_lock');
    }

    /**
     * Returns channel's state key.
     * @return string
     */
    protected function getStateKey()
    {
        return $this->qualifyKey('state');
    }

    /**
     * Returns channel's workers key.
     * @return string
     */
    protected function getWorkersKey()
    {
        return $this->qualifyKey('workers');
    }

    /**
     * Returns channel's worker kill switches key.
     * @return string
     */
    protected function getWorkerKillSwitchesKey()
    {
        return $this->qualifyKey('kill_switches');
    }

    /**
     * Returns channel's standard event queue key.
     * @return string
     */
    protected function getEventsKey()
    {
        return $this->qualifyKey('events');
    }

    /**
     * Returns pinned event queue key.
     * @param  string $pinKey
     * @return string
     */
    protected function getPinnedEventsKey($pinKey)
    {
        return $this->qualifyKey("pinned_events:{$pinKey}");
    }

    /**
     * Returns pin keys' control locks key.
     * This key is used to control opeation on the pin key when its events are
     * being re-dispatched.
     *
     * @return string
     */
    protected function getPinKeysControlLocksKey()
    {
        return $this->qualifyKey("pin_keys_control_locks");
    }

    /**
     * Returns pin keys' worker locks keys.
     * This key is used to prevent two workers from working on the same pin key
     * at the same time.
     *
     * @return string
     */
    protected function getPinKeysWorkerLocksKey()
    {
        return $this->qualifyKey("pin_keys_worker_locks");
    }

    /**
     * Qualifies redis key for this channel.
     * @param  string $key
     * @return string
     */
    protected function qualifyKey($key)
    {
        $qualifiedKey = "event_queue";

        if ($this->tenantKey) {
            $qualifiedKey .= ":{$this->tenantKey}";
        }

        $qualifiedKey .= ":channel:{$this->channelId}:{$key}";
        return $qualifiedKey;
    }

    /**
     * Returns pin key pointer.
     * @param  string $pinKey
     * @return string
     */
    protected function getPinKeyPointer($pinKey)
    {
        return self::PIN_KEY_POINTER_PREFIX . $pinKey;
    }

}
