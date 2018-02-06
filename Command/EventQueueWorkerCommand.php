<?php
namespace Celltrak\EventQueueBundle\Command;

use Symfony\Bundle\FrameworkBundle\Command\ContainerAwareCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Celltrak\EventQueueBundle\Component\EventQueueChannel;
use Celltrak\EventQueueBundle\Component\EventQueueManager;
use Celltrak\EventQueueBundle\Component\EventQueueWorkerFactory;
use Celltrak\EventQueueBundle\Entity\EventQueueWorker as WorkerEntity;
use CTLib\Component\Monolog\Logger;
use CTLib\Component\Console\ConsoleOutputHelper;
use CTLib\Component\Console\ConsoleTable;
use CTLib\Component\Console\ConsoleProcessResult;

/**
 * Manages event queue workers (start/stop).
 * @author Mike Turoff
 */
class EventQueueWorkerCommand extends ContainerAwareCommand
{

    /**
     * {@inheritDoc}
     */
    protected function configure()
    {
        $this
            ->setName('event_queue:worker')
            ->setDescription('Manages event queue workers')
            ->addArgument('action', InputArgument::REQUIRED, "Use 'list' to see available actions")
            ->addArgument('workerId', InputArgument::REQUIRED);
    }

    /**
     * {@inheritDoc}
     */
    protected function execute(InputInterface $input, OutputInterface $output)
    {
        $this->init();

        $action = $input->getArgument('action');

        switch ($action) {
            case 'list':
                return $this->execList($input, $output);

            case 'start':
                return $this->execStartWorker($input, $output);
            case 'kill':
                return $this->execKillWorker($input, $output);

            default:
                throw new \RuntimeException("Invalid action '{$action}'");
        }
    }

    /**
     * Initializes instance service variables.
     * @TODO This will get replaced with __construct once we can define this
     * command as a service with a later version of Symfony.
     * @return void
     */
    protected function init()
    {
        $container = $this->getContainer();

        $this->eventQueueManager = $container->get('event_queue.manager');
        $this->eventQueueWorkerFactory = $container->get('event_queue.worker_factory');
        $this->logger = $container->get('logger');
        $this->isDebug = $container->getParameter('kernel.debug');
    }

    /**
     * Shows possible command actions.
     *
     * @param  InputInterface $input
     * @param  OutputInterface $output
     * @return void
     */
    protected function execList(InputInterface $input, OutputInterface $output)
    {
        $controlActions = [
            'start {workerId}' => 'Starts worker',
            'kill {workerId}' => 'Kills worker'
        ];

        $listActions = [
            'list' => 'Lists these actions'
        ];

        $outputHelper = new ConsoleOutputHelper($output);
        $outputHelper->outputActionList(
            $controlActions,
            $listActions
        );
    }

    /**
     * Starts worker.
     *
     * NOTE: Once a worker is started, it will purposefully run within an
     * infinite loop until stopped by the queue worker monitor service.
     * Usually this action is run asynchronously.
     *
     * @param  InputInterface $input
     * @param  OutputInterface $output
     * @return void
     */
    protected function execStartWorker(
        InputInterface $input,
        OutputInterface $output
    ) {
        $workerId = $input->getArgument('workerId');

        try {
            $worker = $this->eventQueueWorkerFactory->createWorker($workerId);
        } catch (\Exception $e) {
            $this->logger->error((string) $e);
            throw $e;
        }

        // Register a shutdown handler with PHP so fatal errors will gracefully
        // interrupt the worker. It also means the fatal error will get logged
        // within App Log! However, if this is a memory limit exceeded error, we
        // won't try to deactivate and clean up the worker, as we do not have the
        // required memory to do so. Instead, we'll let  the worker remain as is,
        // and it will eventually deactivate normally with a zombie status.
        $shutdownHandler = function($worker) {
            $error = error_get_last();

            if ($error) {
                $error = json_encode($error);
                $this->logger->error("Event Queue Worker fatal error: " . $error);
                if (strpos($error, 'Allowed memory size') === false) {
                    $worker->stop(WorkerEntity::DEACTIVATED_REASON_ERROR, $error);
                }
            }
        };

        register_shutdown_function($shutdownHandler, $worker);

        $output->writeln("");
        $output->writeln("<options=bold>Starting Worker ID {$workerId}...</>");
        $output->writeln("<fg=green>To kill, use event_queue:worker kill.</>");
        $output->writeln("");

        // Notify the worker of its host and system process id when starting.
        $hostname = gethostname();
        $pid = getmypid();

        try {
            $worker->start($hostname, $pid);
        } catch (\Exception $e) {
            $worker->stop(WorkerEntity::DEACTIVATED_REASON_ERROR, (string) $e);
        }
    }

    /**
     * Kills worker.
     *
     * NOTE: Killing workers is usually handled by the event queue worker
     * monitor service. It is provided here for debug and edge cases.
     *
     * @param  InputInterface $input
     * @param  OutputInterface $output
     * @return void
     */
    protected function execKillWorker(
        InputInterface $input,
        OutputInterface $output
    ) {
        $workerId = $input->getArgument('workerId');

        $channel = $this->eventQueueManager->getChannelForWorker($workerId);

        if (is_null($channel)) {
            throw new \RuntimeException("Invalid workerId {$workerId}");
        }

        $killed = $channel->killWorker($workerId);

        if ($killed == false) {
            throw new \RuntimeException("Could not stop workerId {$workerId}");
        }

        $output->writeln("");
        $output->writeln("<options=bold>Stopped workerId {$workerId}</>");
        $output->writeln("");
    }

}
