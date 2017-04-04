<?php
namespace Celltrak\EventQueueBundle\Command;

use Symfony\Bundle\FrameworkBundle\Command\ContainerAwareCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Celltrak\EventQueueBundle\Entity\EventQueueWorker as WorkerEntity;
use Celltrak\EventQueueBundle\Entity\EventQueue;
use Celltrak\EventQueueBundle\Component\EventQueueChannel;
use Celltrak\EventQueueBundle\Component\EventQueueManager;
use Celltrak\EventQueueBundle\Component\EventQueueDispatcher;
use Celltrak\EventQueueBundle\Component\EventQueueWorkerFactory;
use CTLib\Component\Monolog\Logger;
use CTLib\Component\Console\ConsoleOutputHelper;
use CTLib\Component\Console\ConsoleTable;
use CTLib\Component\Console\ConsoleProcessResult;



class EventQueueChannelCommand extends ContainerAwareCommand
{

    /**
     * {@inheritDoc}
     */
    protected function configure()
    {
        $this
            ->setName('event_queue:channel')
            ->setDescription('Manages event queue channels')
            ->addArgument('action', InputArgument::REQUIRED)
            ->addArgument('channel', InputArgument::OPTIONAL)
            ->addOption('max-worker-count', null, InputOption::VALUE_REQUIRED, 'Max worker count used when updating channel')
            ->addOption('max-load-count', null, InputOption::VALUE_REQUIRED, 'Max load count used when updating channel')
            ->addOption('force', 'f', InputOption::VALUE_NONE, 'Skip confirmation prompts');
    }

    protected function execute(InputInterface $input, OutputInterface $output)
    {
        $this->init();

        $action = $input->getArgument('action');

        switch ($action) {
            case 'list':
                return $this->execList($input, $output);

            case 'show-event-config':
                return $this->execShowChannelEventConfig($input, $output);
            case 'show-all-event-config':
                return $this->execShowAllChannelEventConfig($input, $output);

            case 'inspect':
                return $this->execInspectChannel($input, $output);
            case 'inspect-all':
                return $this->execInspectAllChannels($input, $output);

            case 'start':
                return $this->execStartChannel($input, $output);
            case 'start-all':
                return $this->execStartAllChannels($input, $output);
            case 'stop':
                return $this->execStopChannel($input, $output);
            case 'stop-all':
                return $this->execStopAllChannels($input, $output);
            case 'restart':
                return $this->execRestartChannel($input, $output);
            case 'restart-all':
                return $this->execRestartAllChannels($input, $output);

            case 'kill-workers':
                return $this->execKillChannelWorkers($input, $output);

            case 'update':
                return $this->execUpdateChannel($input, $output);

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
        $this->eventQueueDispatcher = $container->get('event_queue.dispatcher');
        $this->eventQueueWorkerFactory = $container->get('event_queue.worker_factory');
        $this->eventQueueProcessingManager = $container->get('event_queue.processing_manager');
        $this->entityManager = $this->eventQueueManager->getEntityManager();
        $this->logger = $container->get('logger');
        $this->isDebug = $container->getParameter('kernel.debug');
    }

    protected function execList(InputInterface $input, OutputInterface $output)
    {
        $showConfigActions = [
            'show-event-config {channel}' => 'Shows event config for channel',
            'show-all-event-config' => 'Shows event config for all channels'
        ];

        $inspectActions = [
            'inspect {channel}' => 'Inspects channel\'s status',
            'inspect-all' => 'Inspects all channel statuses'
        ];

        $controlActions = [
            'start {channel}' => 'Starts channel',
            'start-all' => 'Starts all channels',
            'stop {channel}' => 'Stops channel',
            'stop-all' => 'Stops all channels',
            'restart {channel}' => 'Restarts channel',
            'restart-all' => 'Restarts all channels'
        ];

        $workerActions = [
            'kill-workers {channel}' => 'Kills all active workers in channel'
        ];

        $updateActions = [
            'update {channel}' => 'Updates channel\'s runtime configuration'
        ];

        $listActions = [
            'list' => 'Lists these actions'
        ];


        $outputHelper = new ConsoleOutputHelper($output);
        $outputHelper->outputActionList(
            $showConfigActions,
            $inspectActions,
            $controlActions,
            $workerActions,
            $updateActions,
            $listActions
        );
    }

    protected function execShowChannelEventConfig(
        InputInterface $input,
        OutputInterface $output
    ) {
        $channelId = $input->getArgument('channel');

        if (empty($channelId)) {
            throw new \RuntimeException("{channel} is required for 'show-event-config' action");
        }

        $table = new ConsoleTable;
        $table
            ->addColumn('EVENT', 40)
            ->addColumn('LISTENERS', 80);

        $events = $this->eventQueueManager
            ->getHandledEventsForChannel($channelId);
        sort($events);

        foreach ($events as $event) {
            $listenerIds = $this->eventQueueProcessingManager
                ->getListenerIdsForEvent($event);

            if ($listenerIds) {
                foreach ($listenerIds as $i => $listenerId) {
                    $listenerDisp = str_pad(($i + 1) . '.', 4) . $listenerId;

                    if ($i == 0) {
                        $table->addRecord($event, $listenerDisp);
                    } else {
                        $table->addRecord(null, $listenerDisp);
                    }
                }
            } else {
                $table->addRecord($event, '<No Listeners>');
            }
        }

        $output->writeln("");
        $table->output($output);
        $output->writeln("");
    }

    protected function execShowAllChannelEventConfig(
        InputInterface $input,
        OutputInterface $output
    ) {
        $table = new ConsoleTable;
        $table
            ->addColumn('CHANNEL', 28)
            ->addColumn('EVENT', 40)
            ->addColumn('LISTENERS', 80);

        $allEvents = $this->eventQueueManager->getHandledEventsByChannelId();
        ksort($allEvents);

        foreach ($allEvents as $channelId => $events) {
            sort($events);

            foreach ($events as $event) {
                $listenerIds = $this->eventQueueProcessingManager
                    ->getListenerIdsForEvent($event);

                if ($listenerIds) {
                    foreach ($listenerIds as $i => $listenerId) {
                        $listenerDisp = str_pad(($i + 1) . '.', 4) . $listenerId;

                        if ($i == 0) {
                            $table->addRecord(
                                $channelId,
                                [$event, 'options=bold'],
                                $listenerDisp
                            );
                        } else {
                            $table->addRecord(null, null, $listenerDisp);
                        }
                    }
                } else {
                    $table->addRecord(
                        $channelId,
                        [$event, 'options=bold'],
                        "<No Listeners>"
                    );
                }
            }
        }

        $output->writeln("");
        $table->output($output);
        $output->writeln("");
    }

    protected function execInspectChannel(
        InputInterface $input,
        OutputInterface $output
    ) {
        $channelId = $input->getArgument('channel');

        if (!$channelId) {
            throw new \RuntimeException("{channel} is required for 'inspect' action");
        }

        $channel = $this->eventQueueManager->getChannel($channelId);
        $info = $channel->inspect();

        $outputHelper = new ConsoleOutputHelper($output);

        $status = $info['status'];

        $alertStatuses = [
            'N/A',
            EventQueueChannel::STATUS_STOPPING,
            EventQueueChannel::STATUS_STOPPED
        ];

        if (in_array($status, $alertStatuses)) {
            $statusColor = 'red';
        } else {
            $statusColor = 'green';
        }

        $output->writeln("");
        $outputHelper->outputAttributeValuePair(
            'Status',
            "<fg={$statusColor}>{$status}</>"
        );
        $outputHelper->outputAttributeValuePair(
            'Started (GMT)',
            $info['runningSince']
        );
        $outputHelper->outputAttributeValuePair(
            'Pending Event Count',
            $info['pendingEventCount']
        );
        $outputHelper->outputAttributeValuePair(
            'Worker Count',
            $info['workerCount']
        );
        $outputHelper->outputAttributeValuePair(
            'Max Load Count',
            $info['maxLoadCount']
        );
        $outputHelper->outputAttributeValuePair(
            'Max Worker Count',
            $info['maxWorkerCount']
        );
        $output->writeln("");
    }

    protected function execInspectAllChannels(
        InputInterface $input,
        OutputInterface $output
    ) {
        $table = new ConsoleTable;
        $table
            ->addColumn('CHANNEL', 30)
            ->addColumn('STATUS', 20)
            ->addColumn('STARTED (GMT)', 25)
            ->addColumn('EVENTS', 10)
            ->addColumn('WORKERS', 10)
            ->addColumn('MAX LOAD', 10)
            ->addColumn('MAX WORKERS', 12);

        $info = $this->eventQueueManager->inspect();
        $info = $info['channels'];
        ksort($info);

        foreach ($info as $channelId => $channelInfo) {
            $status = $channelInfo['status'];

            $alertStatuses = [
                'N/A',
                EventQueueChannel::STATUS_STOPPING,
                EventQueueChannel::STATUS_STOPPED
            ];

            if (in_array($status, $alertStatuses)) {
                $statusColor = 'red';
            } else {
                $statusColor = 'green';
            }

            $table->addRecord(
                [$channelId, 'options=bold'],
                [$status, "options=bold;fg={$statusColor}"],
                $channelInfo['runningSince'],
                $channelInfo['pendingEventCount'],
                $channelInfo['workerCount'],
                $channelInfo['maxLoadCount'],
                $channelInfo['maxWorkerCount']
            );
        }

        $output->writeln("");
        $table->output($output);
        $output->writeln("");
    }

    protected function execStartChannel(
        InputInterface $input,
        OutputInterface $output
    ) {
        $channelId = $input->getArgument('channel');

        if (!$channelId) {
            throw new \RuntimeException("{channel} is required for 'start' action");
        }

        $started = $this->eventQueueManager->startChannel($channelId, $error);

        $result = new ConsoleProcessResult($channelId);

        if ($started) {
            $result->success("STARTED");
        } else {
            $result->failure($error);
        }

        $output->writeln("");
        (new ConsoleOutputHelper($output))->outputProcessResult($result);
        $output->writeln("");
    }

    protected function execStartAllChannels(
        InputInterface $input,
        OutputInterface $output
    ) {
        $results = $this->eventQueueManager->startAllChannels();
        $outputHelper = new ConsoleOutputHelper($output);

        $output->writeln("");

        foreach ($results as $channelId => list($succeeded, $error)) {
            $result = new ConsoleProcessResult($channelId);

            if ($succeeded) {
                $result->success('STARTED');
            } else {
                $result->failure($error);
            }

            $outputHelper->outputProcessResult($result);
        }

        $output->writeln("");
    }

    protected function execStopChannel(
        InputInterface $input,
        OutputInterface $output
    ) {
        $channelId = $input->getArgument('channel');
        $force = $input->getOption('force');

        if (!$channelId) {
            throw new \RuntimeException("{channel} is required for 'stop' action");
        }

        if (!$force) {
            $confirmMsg = "<options=bold>Are you sure you want to stop channel {$channelId}?</>"
                        . "\n<fg=red>WARNING: No events handled by this channel will be processed"
                        . " until it is started again.</>"
                        . "\n\n<options=bold>Really Stop Channel {$channelId}? Y/n</> ";

            $dialog     = $this->getHelperSet()->get('dialog');
            $continue   = $dialog->askConfirmation($output, "\n{$confirmMsg}");

            if (!$continue) {
                $output->writeln("");
                $output->writeln("<options=bold>Channel {$channelId} *NOT* Stopped</>");
                $output->writeln("");
                return;
            }
        }

        $stopped = $this->eventQueueManager->stopChannel($channelId, $error);

        $result = new ConsoleProcessResult($channelId);

        if ($stopped) {
            $result->success('STOPPED');
        } else {
            $result->failure($error);
        }

        $output->writeln("");
        (new ConsoleOutputHelper($output))->outputProcessResult($result);
        $output->writeln("");
    }

    protected function execStopAllChannels(
        InputInterface $input,
        OutputInterface $output
    ) {
        $force = $input->getOption('force');

        if (!$force) {
            $confirmMsg = "<options=bold>Are you sure you want to stop all channels?</>"
                        . "\n<fg=red>WARNING: No events will be processed until the channels"
                        . " are started again.</>"
                        . "\n\n<options=bold>Really Stop All Channels? Y/n</> ";

            $dialog     = $this->getHelperSet()->get('dialog');
            $continue   = $dialog->askConfirmation($output, "\n{$confirmMsg}");

            if (!$continue) {
                $output->writeln("");
                $output->writeln("<options=bold>*NO* Channels were Stopped</>");
                $output->writeln("");
                return;
            }
        }

        $results = $this->eventQueueManager->stopAllChannels();
        $outputHelper = new ConsoleOutputHelper($output);

        $output->writeln("");

        foreach ($results as $channelId => list($succeeded, $error)) {
            $result = new ConsoleProcessResult($channelId);

            if ($succeeded) {
                $result->success('STOPPED');
            } else {
                $result->failure($error);
            }

            $outputHelper->outputProcessResult($result);
        }

        $output->writeln("");
    }

    protected function execRestartChannel(
        InputInterface $input,
        OutputInterface $output
    ) {
        $channelId = $input->getArgument('channel');
        $force = $input->getOption('force');

        if (!$channelId) {
            throw new \RuntimeException("{channel} is required for 'restart' action");
        }

        if (!$force) {
            $confirmMsg = "<options=bold>Really Restart Channel {$channelId}? Y/n</> ";

            $dialog     = $this->getHelperSet()->get('dialog');
            $continue   = $dialog->askConfirmation($output, "\n{$confirmMsg}");

            if (!$continue) {
                $output->writeln("");
                $output->writeln("<options=bold>Channel {$channelId} *NOT* Restarted</>");
                $output->writeln("");
                return;
            }
        }

        $restarted = $this->eventQueueManager->restartChannel($channelId, $error);

        if (!$restarted) {
            throw new \RuntimeException("Cannot restart '{$channelId}' channel: {$error}");
        }

        $output->writeln("");
        $output->writeln("<options=bold>Restarted '{$channelId}' channel</>");
        $output->writeln("");
    }

    protected function execRestartAllChannels(
        InputInterface $input,
        OutputInterface $output
    ) {
        $force = $input->getOption('force');

        if (!$force) {
            $confirmMsg = "<options=bold>Really Restart All Channels? Y/n</> ";

            $dialog     = $this->getHelperSet()->get('dialog');
            $continue   = $dialog->askConfirmation($output, "\n{$confirmMsg}");

            if (!$continue) {
                $output->writeln("");
                $output->writeln("<options=bold>*NO* Channels were Restarted</>");
                $output->writeln("");
                return;
            }
        }

        $results = $this->eventQueueManager->restartAllChannels();
        $outputHelper = new ConsoleOutputHelper($output);

        $output->writeln("");

        foreach ($results as $channelId => list($succeeded, $error)) {
            $result = new ConsoleProcessResult($channelId);

            if ($succeeded) {
                $result->success('RESTARTED');
            } else {
                $result->failure($error);
            }

            $outputHelper->outputProcessResult($result);
        }

        $output->writeln("");
    }

    protected function execKillChannelWorkers(
        InputInterface $input,
        OutputInterface $output
    ) {
        $channelId = $input->getArgument('channel');

        if (!$channelId) {
            throw new \RuntimeException("{channel} is required for 'kill-workers' action");
        }

        $channel = $this->eventQueueManager->getChannel($channelId);
        $controlLockId = $channel->acquireControlLock();

        if (!$controlLockId) {
            throw new \RuntimeException("Another process is already controlling the '{$channelId}' channel");
        }

        try {
            $error = null;
            $result = $channel->killAllWorkers($controlLockId, $error);
        } finally {
            $channel->releaseControlLock($controlLockId);
        }

        if ($result === false) {
            throw new \RuntimeException("Cannot kill all active workers for '{$channelId}' channel: {$error}");
        }

        $output->writeln("");
        $output->writeln("<options=bold>Killed {$result} worker(s) for '{$channelId}' channel</>");
        $output->writeln("");
    }

    protected function execUpdateChannel(
        InputInterface $input,
        OutputInterface $output
    ) {
        $channelId = $input->getArgument('channel');

        if (!$channelId) {
            throw new \RuntimeException("{channel} is required for 'update' action");
        }

        $channel = $this->eventQueueManager->getChannel($channelId);

        $maxWorkerCount = $input->getOption('max-worker-count');

        if (!is_null($maxWorkerCount)) {
            $intValue = (int) $maxWorkerCount;

            if ($intValue != $maxWorkerCount || $intValue <= 0) {
                throw new \RuntimeException("{max-worker-count} must be integer greater than 0");
            }

            $maxWorkerCount = $intValue;
        }

        $maxLoadCount = $input->getOption('max-load-count');

        if (!is_null($maxLoadCount)) {
            $intValue = (int) $maxLoadCount;

            if ($intValue != $maxLoadCount || $intValue <= 0) {
                throw new \RuntimeException("{max-load-count} must be integer great than 0");
            }

            $maxLoadCount = $intValue;
        }

        if (!$maxWorkerCount && !$maxLoadCount) {
            throw new \RuntimeException("No channel updates specified");
        }

        if ($maxWorkerCount) {
            $channel->setMaxWorkerCount($maxWorkerCount);
        }

        if ($maxLoadCount) {
            $channel->setMaxLoadCount($maxLoadCount);
        }

        $output->writeln("");
        $output->writeln("<options=bold>Updated Channel {$channelId}</>");
        $output->writeln("");
    }


    protected function abridgeAndPad($str, $length)
    {
        $str = substr($str, 0, $length - 1);
        $str = str_pad($str, $length);
        return $str;
    }

}
