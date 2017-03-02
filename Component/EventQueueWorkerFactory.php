<?php
namespace Celltrak\EventQueueBundle\Component;

use Doctrine\Bundle\DoctrineBundle\Registry as DoctrineRegistry;
use Doctrine\ORM\EntityManager;
use CTLib\Component\Monolog\Logger;


/**
 * Constructs EventQueueWorker instances.
 * @author Mike Turoff
 */
class EventQueueWorkerFactory
{

    /**
     * @var EventQueueManager
     */
    protected $queueManager;

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
     * @var integer
     */
    protected $maxListenerAttempts;

    /**
     * @var integer
     */
    protected $memoryUsagePercentage;

    /**
     * @var integer
     */
    protected $sleepSeconds;


    /**
     * @param EventQueueManager $queueManager
     * @param EventQueueProcessingManager $processingManager
     * @param EventQueueDispatcher $eventQueueDispatcher
     * @param EntityManager $entityManager
     * @param DoctrineRegistry $doctrine
     * @param Logger $logger
     * @param integer $maxListenerAttempts
     * @param integer $memoryUsagePercentage
     * @param integer $sleepSeconds
     */
    public function __construct(
        EventQueueManager $queueManager,
        EventQueueProcessingManager $processingManager,
        EventQueueDispatcher $eventQueueDispatcher,
        EntityManager $entityManager,
        DoctrineRegistry $doctrine,
        Logger $logger,
        $maxListenerAttempts,
        $memoryUsagePercentage,
        $sleepSeconds
    ) {
        $this->queueManager = $queueManager;
        $this->processingManager = $processingManager;
        $this->eventQueueDispatcher = $eventQueueDispatcher;
        $this->entityManager = $entityManager;
        $this->doctrine = $doctrine;
        $this->logger = $logger;
        $this->maxListenerAttempts = $maxListenerAttempts;
        $this->memoryUsagePercentage = $memoryUsagePercentage;
        $this->sleepSeconds = $sleepSeconds;
    }

    /**
     * @param integer $workerId
     * @return EventQueueWorker
     * @throws InvalidArgumentException If workerId is invalid
     */
    public function createWorker($workerId)
    {
        $channel = $this->queueManager->getChannelForWorker($workerId);

        if (!$channel) {
            throw new \InvalidArgumentException("Invalid workerId {$workerId}");
        }

        $worker =
            new EventQueueWorker(
                $workerId,
                $channel,
                $this->processingManager,
                $this->eventQueueDispatcher,
                $this->entityManager,
                $this->doctrine,
                $this->logger
            );

        if ($this->maxListenerAttempts) {
            $worker->setMaxListenerAttempts($this->maxListenerAttempts);
        }

        if ($this->memoryUsagePercentage) {
            $worker->setMemoryUsagePercentage($this->memoryUsagePercentage);
        }

        if ($this->sleepSeconds) {
            $worker->setSleepSeconds($this->sleepSeconds);
        }

        return $worker;
    }

}
