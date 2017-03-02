<?php
namespace Celltrak\EventQueueBundle\Controller;

use Celltrak\EventQueueBundle\Entity\EventQueueWorker;
use Celltrak\EventQueueBundle\Component\EventQueueManager;
use CTLib\Component\Console\SymfonyCommandExecutorFactoryInterface;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\HttpFoundation\Response;
use Doctrine\ORM\EntityManager;
use CTLib\Component\Monolog\Logger;


/**
 * Event Queue Worker REST API.
 *
 * @author Mike Turoff
 */
class EventQueueWorkerController
{

    /**
     * @var EventQueueManager
     */
    protected $eventQueueManager;

    /**
     * @var SymfonyCommandExecutorFactoryInterface
     */
    protected $commandExecutorFactory;

    /**
     * @var EntityManager
     */
    protected $entityManager;

    /**
     * @var Logger
     */
    protected $logger;

    /**
     * Maximum number of seconds allowed to wait for worker to provision.
     * @var integer
     */
    protected $workerProvisionTimeout;


    /**
     * @param EventQueueManager $eventQueueManager
     * @param SymfonyCommandExecutorFactoryInterface $commandExecutorFactory
     * @param EntityManager $entityManager
     * @param Logger $logger
     * @param integer $workerProvisionTimeout
     */
    public function __construct(
        EventQueueManager $eventQueueManager,
        SymfonyCommandExecutorFactoryInterface $commandExecutorFactory,
        EntityManager $entityManager,
        Logger $logger,
        $workerProvisionTimeout = 0
    ) {
        $this->eventQueueManager        = $eventQueueManager;
        $this->commandExecutorFactory   = $commandExecutorFactory;
        $this->entityManager            = $entityManager;
        $this->logger                   = $logger;
        $this->workerProvisionTimeout   = $workerProvisionTimeout;
    }

    /**
     * Provisions worker for channel.
     * @param string  $channelId
     * @return Response
     */
    public function provisionAction($channelId)
    {
        // Provisioning breaks down into two steps. First, add a new
        // EventQueueWorker entry into the database. Then fork the worker
        // process using a command.

        $this->logger->startThread();
        $this->logger->debug("EventQueueWorkerController: provisioning worker for '{$channelId}' channel");

        try {
            $channel = $this->eventQueueManager->getChannel($channelId);
        } catch (\Exception $e) {
            $this->logger->error((string) $e);
            return new Response('Channel does not exist', 404);
        }

        if (!$channel->isRunning()) {
            $this->logger->warn("EventQueueWorkerController: channel isn't running so cannot provision worker");
            return new Response('Channel is not running', 500);
        }

        $worker = new EventQueueWorker([
            'channel' => $channelId,
            'status' => EventQueueWorker::STATUS_PROVISIONING
        ]);
        $this->entityManager->insert($worker);

        $workerId = $worker->getWorkerId();

        // Fork worker process.
        $commandExecutor =
            $this
            ->commandExecutorFactory
            ->createCommandExecutor('event_queue:control')
            ->addArgument('start-worker')
            ->addArgument($workerId)
            ->addOption('quiet');

        $commandExecutor->execAsynchronous();

        // Verify that worker process started successfully by continually
        // checking whether worker has logged its activation.
        $giveupTime = time() + (int) $this->workerProvisionTimeout;

        do {
            // Sleep to buy some time for the worker process to start.
            sleep(1);
            $started = $channel->hasWorker($workerId);
        } while (!$started && time() < $giveupTime);

        if ($started) {
            $this->logger->debug("EventQueueWorkerController: workerId {$workerId} provisioned successfully");
            $httpStatus = 200;
            $responseBody = json_encode(['workerId' => $workerId]);
        } else {
            $this->logger->debug("EventQueueWorkerController: workerId {$workerId} failed to provision");

            // Log that the worker failed to provision.
            $updateFields = [
                'status' => EventQueueWorker::STATUS_FAILED_PROVISIONING
            ];
            $this->entityManager->updateForFields($worker, $updateFields);

            $httpStatus = 500;
            $responseBody = 'Cannot provision worker';
        }

        return new Response($responseBody, $httpStatus);
    }

    /**
     * Kills specified worker.
     * @param  Request $request
     * @param  string  $channelId
     * @param  integer  $workerId
     * @return Response
     */
    public function killAction(Request $request, $channelId, $workerId)
    {
        $this->logger->startThread();
        $this->logger->debug("EventQueueWorkerController: killing workerId {$workerId} in '{$channelId}' channel");

        $channel = $this->eventQueueManager->getChannel($channelId);

        $isZombie = $request->get('zombie');

        // Return HTTP 200 regardless of whether worker was actually killed
        // (deactivated). We're doing this even though it's not technically
        // correct because the Worker Monitor daemon will get blocked if server
        // returns anything other than 200. It will keep attempting to kill the
        // same worker over and over again. This issue should only occur if the
        // worker deactivates at the exact same time the Worker Monitor is
        // iterating through the workers it should kill. This is an edge case,
        // but the negative of making this not-so-technically-correct change
        // is less than having the Monitor get stuck. Also, since this is an
        // 11th hour fix, it's much safer to make this change here rather than
        // risk opening up the Monitor itself. No real harm at this point will
        // occur if the server incorrectly reports a 200 even though the worker
        // failed to deactivate for this request. Eventually we should restore
        // this logic to return HTTP 500 and 400 when appropriate and fix the
        // Monitor's HTTP return handling.

        try {
            if ($isZombie) {
                // Queue assumes that the worker is no longer running so there's
                // no use in trying to kill it through the normal #killWorker.
                // #killWorker relies on the running worker to gracefully handle
                // its own demise. Instead, directly call #deactivateWorker on
                // the worker's behalf.
                $deactivatedReason = EventQueueWorker::DEACTIVATED_REASON_ZOMBIE;
                $killed = $channel->deactivateWorker($workerId, $deactivatedReason);
            } else {
                $killed = $channel->killWorker($workerId);
            }

            if (!$killed) {
                $this->logger->debug("EventQueueWorkerController: worker already killed but returning HTTP 200 to prevent block in Monitor daemon");
            }
        } catch (\Exception $e) {
            $this->logger->error((string) $e);
        }

        return new Response("Worker ID {$workerId} killed");
    }

}
