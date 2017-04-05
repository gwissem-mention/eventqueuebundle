<?php
namespace Celltrak\EventQueueBundle\Component;

use Celltrak\RedisBundle\Component\Client\CelltrakRedis;
use Doctrine\ORM\EntityManager;
use CTLib\Component\Monolog\Logger;

/**
 * Manages the queue workers that notify the registered listeners about
 * the events.
 *
 * @author Mike Turoff <mturoff@celltrak.com>
 */
class EventQueueManager
{

    /**
     * Redis key for Worker Monitor daemon heartbeat.
     */
    const WORKER_MONITOR_HEARTBEAT_KEY  = 'event_queue:worker_monitor:heartbeat';


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
     * Set of registered channel configurations.
     * @var array
     */
    protected $channelConfigs;

    /**
     * Set of registered events handled by this queue.
     * @var array
     */
    protected $handledEvents;

    /**
     * Set of instantiated EventQueueChannel instances.
     * @var array
     */
    protected $channels;

    /**
     * Token used to run event queue in multi-tenant mode.
     * @var string
     */
    protected $tenantKey;

    /**
     * The number of seconds since a worker's last check-in before it's
     * considered to be a zombie process. This is passed thru to
     * EventQueueChannel instances.
     * @var integer
     */
    protected $zombieWorkerIdleSeconds;


    /**
     * @param CelltrakRedis $redis
     * @param EntityManager $entityManager
     * @param Logger $logger
     */
    public function __construct(
        CelltrakRedis $redis,
        EntityManager $entityManager,
        Logger $logger
    ) {
        $this->redis                = $redis;
        $this->entityManager        = $entityManager;
        $this->logger               = $logger;
        $this->channelConfigs       = [];
        $this->handledEvents        = [];
        $this->channels             = [];
    }

    /**
     * Registers queue channel configuration.
     * @param string $channelId Channel's unique identifier.
     * @param array $handledEvents Events handled by the channel.
     * @param integer $defaultMaxWorkers Default number of workers allowed to
     *                                   run concurrently in this channel.
     * @param integer $defaultMaxLoad    Default number of events per worker
     *                                   before attempting to provision another
     *                                   worker.
     * @return void
     */
    public function registerChannel(
        $channelId,
        array $handledEvents,
        $defaultMaxWorkers,
        $defaultMaxLoad
    ) {
        if (isset($this->channelConfigs[$channelId])) {
            throw new \InvalidArgumentException("Channel already registered for ID '{$channelId}'");
        }

        if (!$handledEvents) {
            throw new \InvalidArgumentException("At least 1 handled event is required");
        }

        foreach ($handledEvents as $event) {
            if ($this->isHandledEvent($event)) {
                throw new \InvalidArgumentException("'{$event}' event is already handled by another registered channel");
            }
        }

        if (!is_int($defaultMaxWorkers) || $defaultMaxWorkers <= 0) {
            throw new \InvalidArgumentException('$defaultMaxWorkers must be integer greater than 0');
        }

        if (!is_int($defaultMaxLoad) || $defaultMaxLoad <= 0) {
            throw new \InvalidArgumentException('$defaultMaxLoad must be integer greater than 0');
        }

        $config = new \stdClass;
        $config->defaultMaxWorkers = $defaultMaxWorkers;
        $config->defaultMaxLoad = $defaultMaxLoad;
        $config->handledEvents = $handledEvents;
        $this->channelConfigs[$channelId] = $config;

        $handledEvents = array_fill_keys($handledEvents, $channelId);
        $this->handledEvents += $handledEvents;
    }

    /**
     * Starts queue processing for all enabled channels.
     *
     * @return array    Returns [$channelId => [$started, $error], ...]
     */
    public function startAllChannels()
    {
        $results = [];

        foreach ($this->getChannelIds() as $channelId) {
            $error = null;
            $started = $this->startChannel($channelId, $error);

            $results[$channelId] = [$started, $error];
        }

        return $results;
    }

    /**
     * Stops queue processing for all enabled channels.
     *
     * @return array    Returns [$channelId => [$stopped, $error], ...]
     */
    public function stopAllChannels()
    {
        $results = [];

        foreach ($this->getChannelIds() as $channelId) {
            $error = null;
            $stopped = $this->stopChannel($channelId, $error);

            $results[$channelId] = [$stopped, $error];
        }

        return $results;
    }

    /**
     * Restarts queue processing for all enabled channels.
     *
     * @return array    Returns [$channelId => [$restarted, $error], ...]
     */
    public function restartAllChannels()
    {
        $results = [];

        foreach ($this->getChannelIds() as $channelId) {
            $error = null;
            $restarted = $this->restartChannel($channelId, $error);

            $results[$channelId] = [$restarted, $error];
        }

        return $results;
    }

    /**
     * Starts queue processing for specified channel.
     *
     * @param string $channelId
     * @param string $error     Set internally on failure.
     * @return boolean          Indicates whether start was successful.
     */
    public function startChannel($channelId, &$error = null)
    {
        $channel = $this->getChannel($channelId);

        $controlLockId = $channel->acquireControlLock();

        if (!$controlLockId) {
            $error = "Another process is already controlling the '{$channelId}' channel";
            return false;
        }

        try {
            $error = null;
            $started = $channel->start($controlLockId, $error);
        } finally {
            $channel->releaseControlLock($controlLockId);
        }

        return $started;
    }

    /**
     * Stops queue processing for specified channel.
     *
     * @param string $channelId
     * @param string $error     Set internally on failure.
     * @return boolean          Indicates whether stop was successful.
     */
    public function stopChannel($channelId, &$error = null)
    {
        $channel = $this->getChannel($channelId);

        $controlLockId = $channel->acquireControlLock();

        if (!$controlLockId) {
            $error = "Another process is already controlling the '{$channelId}' channel";
            return false;
        }

        try {
            $error = null;
            $stopped = $channel->stop($controlLockId, $error);
        } finally {
            $channel->releaseControlLock($controlLockId);
        }

        return $stopped;
    }

    /**
     * Restarts queue processing for specified channel.
     *
     * @param string $channelId
     * @param string $error     Set internally on failure.
     * @return boolean          Indicates whether restart was successful.
     */
    public function restartChannel($channelId, &$error = null)
    {
        $channel = $this->getChannel($channelId);

        $controlLockId = $channel->acquireControlLock();

        if (!$controlLockId) {
            $error = "Another process is already controlling the '{$channelId}' channel";
            return false;
        }

        try {
            $error = null;
            $restarted = $channel->restart($controlLockId, $error);
        } finally {
            $channel->releaseControlLock($controlLockId);
        }

        return $restarted;
    }

    /**
     * Returns the operational metrics for the queue and its channels.
     *
     * @return array
     */
    public function inspect()
    {
        $info = [];

        $info['isWorkerMonitorRunning'] = $this->isWorkerMonitorRunning();
        $info['channels'] = [];

        foreach ($this->getChannelIds() as $channelId) {
            $channel = $this->getChannel($channelId);
            $info['channels'][$channelId] = $channel->inspect();
        }

        return $info;
    }

    /**
     * Indicates whether the Worker Monitor daemon service is running.
     * @return boolean
     */
    public function isWorkerMonitorRunning()
    {
        return $this->redis->exists(self::WORKER_MONITOR_HEARTBEAT_KEY);
    }

    /**
     * Returns EventQueueChannel instance for $channelId.
     * @param  string $channelId
     * @return EventQueueChannel
     */
    public function getChannel($channelId)
    {
        if (isset($this->channels[$channelId])) {
            return $this->channels[$channelId];
        }

        if (!$this->hasChannel($channelId)) {
            throw new \InvalidArgumentException("'{$channelId}' is not a registered channel");
        }

        $channelConfig = $this->channelConfigs[$channelId];

        $channel =
            new EventQueueChannel(
                $channelId,
                $channelConfig->defaultMaxWorkers,
                $channelConfig->defaultMaxLoad,
                $this->redis,
                $this->entityManager,
                $this->logger);

        if ($this->tenantKey) {
            $channel->setTenantKey($this->tenantKey);
        }

        if ($this->zombieWorkerIdleSeconds) {
            $channel->setZombieWorkerIdleSeconds($this->zombieWorkerIdleSeconds);
        }

        $this->channels[$channelId] = $channel;
        return $channel;
    }

    /**
     * Indicates whether channel is registered to this queue.
     * @param string $channelId
     * @return boolean
     */
    public function hasChannel($channelId)
    {
        return isset($this->channelConfigs[$channelId]);
    }

    /**
     * Returns set of registered channel IDs.
     * @return array
     */
    public function getChannelIds()
    {
        return array_keys($this->channelConfigs);
    }

    /**
     * Returns handled events aggregated by channel id.
     * @return array
     */
    public function getHandledEventsByChannelId()
    {
        $handledEventsByChannelId = [];

        foreach ($this->handledEvents as $eventName => $channelId) {
            $handledEventsByChannelId[$channelId][] = $eventName;
        }

        return $handledEventsByChannelId;
    }

    public function getHandledEventsForChannel($channelId)
    {
        if ($this->hasChannel($channelId) == false) {
            throw new \InvalidArgumentException("'{$channelId}' is not a registered channel");
        }

        $channelConfig = $this->channelConfigs[$channelId];
        return $channelConfig->handledEvents;
    }

    /**
     * Indicates whether a channel has been configured to handle specified event.
     * @param string $eventName
     * @return boolean
     */
    public function isHandledEvent($eventName)
    {
        return isset($this->handledEvents[$eventName]);
    }

    /**
     * Returns EventQueueChannel instance handling specified event.
     * @param  string $eventName
     * @return EventQueueChannel|null
     */
    public function getChannelForEvent($eventName)
    {
        if (!$this->isHandledEvent($eventName)) {
            return null;
        }

        $channelId = $this->handledEvents[$eventName];
        return $this->getChannel($channelId);
    }

    /**
     * Returns EventQueueChannel instance assigned for specified worker.
     * @param integer $workerId
     * @return EventQueueChannel|null
     */
    public function getChannelForWorker($workerId)
    {
        $worker =
            $this
            ->entityManager
            ->getRepository('CelltrakEventQueueBundle:EventQueueWorker')
            ->_find($workerId);

        if (!$worker) {
            return null;
        }

        $channelId = $worker->getChannel();
        return $this->getChannel($channelId);
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
     * Returns EntityManager used by the queue.
     * @return EntityManager
     */
    public function getEntityManager()
    {
        return $this->entityManager;
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

}
