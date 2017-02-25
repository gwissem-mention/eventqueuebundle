<?php
namespace Celltrak\EventQueueBundle\Command;

use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Celltrak\EventQueueBundle\Entity\EventQueueWorker as WorkerEntity;
use Celltrak\EventQueueBundle\Entity\EventQueue;
use Celltrak\EventQueueBundle\Component\EventQueueChannel;
use Celltrak\EventQueueBundle\Component\EventQueueWorker;
use CTLib\Component\Console\BaseCommand;

/**
 * Control event queue.
 *
 * @author Mike Turoff
 */
class EventQueueCommand extends BaseCommand
{

    /**
     * {@inheritDoc}
     */
    protected function configure()
    {
        parent::configure();

        $this
            ->setDescription('Control event queue')
            ->addArgument('siteId', InputArgument::REQUIRED)
            ->addArgument('action', InputArgument::REQUIRED)
            ->addArgument('id', InputArgument::OPTIONAL)
            ->addOption('data', null, InputOption::VALUE_REQUIRED, 'Data when dispatching event')
            ->addOption('reference-key', null, InputOption::VALUE_REQUIRED, 'Reference Key when dispatching event')
            ->addOption('pin-key', null, InputOption::VALUE_REQUIRED, 'Pin Key when dispatching event')
            ->addOption('use-fresh-worker', null, InputOption::VALUE_NONE, 'Forces creation of fresh workers when dispatching/retrying events')
            ->addOption('max-worker-count', null, InputOption::VALUE_REQUIRED, 'Max worker count used when updating channel')
            ->addOption('max-load-count', null, InputOption::VALUE_REQUIRED, 'Max load count used when updating channel')
            ->addOption('force', 'f', InputOption::VALUE_NONE, 'Skip confirmation prompts');
    }

    /**
     * {@inheritDoc}
     */
    protected function execute(InputInterface $input, OutputInterface $output)
    {
        $action = $input->getArgument('action');

        switch ($action) {
            case 'list':
                return $this->execList($input, $output);

            case 'inspect-queue':
                return $this->execInspectQueue($input, $output);
            case 'inspect-channel':
                return $this->execInspectChannel($input, $output);

            case 'start-channel':
                return $this->execStartChannel($input, $output);
            case 'stop-channel':
                return $this->execStopChannel($input, $output);
            case 'restart-channel':
                return $this->execRestartChannel($input, $output);
            case 'start-all-channels':
                return $this->execStartAllChannels($input, $output);
            case 'stop-all-channels':
                return $this->execStopAllChannels($input, $output);
            case 'restart-all-channels':
                return $this->execRestartAllChannels($input, $output);

            case 'start-worker':
                return $this->execStartWorker($input, $output);
            case 'stop-worker':
                return $this->execStopWorker($input, $output);
            case 'stop-all-workers':
                return $this->execStopAllWorkers($input, $output);

            case 'retry-event':
                return $this->execRetryEvent($input, $output);
            case 'redispatch-event':
                return $this->execReDispatchEvent($input, $output);
            case 'dispatch-event':
                return $this->execDispatchEvent($input, $output);

            case 'update-channel':
                return $this->execUpdateChannel($input, $output);

            default:
                throw new \RuntimeException("Invalid action '{$action}'");
        }
    }

    /**
     * Lists out possible command actions.
     *
     * @param  InputInterface $input
     * @param  OutputInterface $output
     * @return void
     */
    protected function execList(InputInterface $input, OutputInterface $output)
    {
        $padLength      = 30;
        $dividerLength  = 70;

        $msg = "\n <bg=blue;options=bold> Available Actions: </>"
             . "\n"

             . "\n" . str_repeat("-", $dividerLength)
             . "\n  " . $this->formatActionInfo("inspect-queue", "Inspects status of queue")
             . "\n  " . $this->formatActionInfo("inspect-channel {channel}", "Inspects status of channel")

             . "\n" . str_repeat("-", $dividerLength)
             . "\n  " . $this->formatActionInfo("start-channel {channel}", "Starts channel processing")
             . "\n  " . $this->formatActionInfo("stop-channel {channel}", "Stops channel processing")
             . "\n  " . $this->formatActionInfo("restart-channel {channel}", "Restarts channel processing")
             . "\n  " . $this->formatActionInfo("start-all-channels", "Starts *all* channel processing")
             . "\n  " . $this->formatActionInfo("stop-all-channels", "Stops *all* channel processing")
             . "\n  " . $this->formatActionInfo("restart-all-channels", "Restarts *all* channel processing")

             . "\n" . str_repeat("-", $dividerLength)
             . "\n  " . $this->formatActionInfo("start-worker {workerId}", "Starts worker")
             . "\n  " . $this->formatActionInfo("stop-worker {workerId}", "Stops worker")
             . "\n  " . $this->formatActionInfo("stop-all-workers {channel}", "Stops all channel workers")

             . "\n" . str_repeat("-", $dividerLength)
             . "\n  " . $this->formatActionInfo("retry-event {queueId}", "Retries failed event")
             . "\n  " . $this->formatActionInfo("redispatch-event {queueId}", "Re-dispatches event")
             . "\n  " . $this->formatActionInfo("dispatch-event {eventName}", "Dispatches new event")

             . "\n" . str_repeat("-", $dividerLength)
             . "\n  " . $this->formatActionInfo("update-channel {channel}", "Updates channel configuration")

             . "\n" . str_repeat("-", $dividerLength)
             . "\n  " . $this->formatActionInfo("list", "Lists these actions")
             . "\n" . str_repeat("-", $dividerLength)
             . "\n\n";

        $output->writeln($msg);
    }

    /**
     * Formats action information.
     *
     * @param string $action
     * @param string $actionDescription
     * @return string
     */
    protected function formatActionInfo($action, $description)
    {
        $padLength = 30;

        return "<options=bold>" . str_pad($action, $padLength) . "</>"
                . $description;
    }

    /**
     * Inspects operational state of entire event queue.
     *
     * @param  InputInterface $input
     * @param  OutputInterface $output
     * @return void
     */
    protected function execInspectQueue(
        InputInterface $input,
        OutputInterface $output
    ) {
        $info = $this->getService('event_queue.manager')->inspect();

        $output->writeln("");
        $output->writeln(str_repeat('-', 80));
        $msg = "<options=bold> Worker Monitor Service is ";

        if ($info['isWorkerMonitorRunning']) {
            $msg .= "<bg=green;fg=white;options=bold> RUNNING </>";
        } else {
            $msg .= "<bg=red;fg=white;options=bold> STOPPED </>";
        }

        $output->writeln($msg);
        $output->writeln(str_repeat('-', 80));

        foreach ($info['channels'] as $channelId => $channelInfo) {
            $output->writeln("");
            $output->writeln("<options=bold;bg=blue> {$channelId} </>");
            $output->writeln($this->formatChannelInspectionInfo($channelInfo));
        }

        $output->writeln("");
    }

    /**
     * Inspects operational state of a specific channel.
     *
     * @param  InputInterface $input
     * @param  OutputInterface $output
     * @return void
     */
    protected function execInspectChannel(
        InputInterface $input,
        OutputInterface $output
    ) {
        $channelId = $input->getArgument('id');

        if (!$channelId) {
            throw new \RuntimeException("{channel} is required for 'inspect-channel' action");
        }

        $channel =
            $this
            ->getService('event_queue.manager')
            ->getChannel($channelId);

        $channelInfo = $channel->inspect();

        $output->writeln("");
        $output->writeln($this->formatChannelInspectionInfo($channelInfo));
        $output->writeln("");
    }

    /**
     * Helper method to format channel's info returned from inspect.
     *
     * @param array $channelInfo
     * @return string
     */
    protected function formatChannelInspectionInfo(array $channelInfo)
    {
        $alertStatuses = [
            'N/A',
            EventQueueChannel::STATUS_STOPPING,
            EventQueueChannel::STATUS_STOPPED
        ];

        if (in_array($channelInfo['status'], $alertStatuses)) {
            $statusColor = 'red';
        } else {
            $statusColor = 'green';
        }

        $statusValue = "<fg={$statusColor}>{$channelInfo['status']}</>";

        $msg = $this->formatChannelInspectionLabel('Status') . $statusValue
             . "\n"
             . $this->formatChannelInspectionLabel("Running Since")
             . $channelInfo['runningSince']
             . "\n"
             . $this->formatChannelInspectionLabel("Pending Event Count")
             . $channelInfo['pendingEventCount']
             . "\n"
             . $this->formatChannelInspectionLabel("Worker Count")
             . $channelInfo['workerCount']
             . "\n"
             . $this->formatChannelInspectionLabel("Max Load Count")
             . $channelInfo['maxLoadCount']
             . "\n"
             . $this->formatChannelInspectionLabel("Max Worker Count")
             . $channelInfo['maxWorkerCount'];

        return $msg;
    }

    /**
     * Helper method to format channel info label for inspect.
     *
     * @param string $label
     * @return string
     */
    protected function formatChannelInspectionLabel($label)
    {
        $labelPadLength = 40;
        $labelPadChar = '.';
        $labelPadRepeat = $labelPadLength - strlen($label);

        return "<options=bold> {$label}</>"
                . str_repeat($labelPadChar, $labelPadRepeat);
    }

    /**
     * Starts queue processing for channel.
     *
     * @param  InputInterface $input
     * @param  OutputInterface $output
     * @return void
     */
    protected function execStartChannel(
        InputInterface $input,
        OutputInterface $output
    ) {
        $channelId = $input->getArgument('id');

        if (!$channelId) {
            throw new \RuntimeException("{channel} is required for 'start-channel' action");
        }

        $started =
            $this
            ->getService('event_queue.manager')
            ->startChannel($channelId, $error);

        if (!$started) {
            throw new \RuntimeException("Cannot start '{$channelId}' channel: {$error}");
        }

        $output->writeln("");
        $output->writeln("<options=bold>Started '{$channelId}' channel</>");
        $output->writeln("");
    }

    /**
     * Stops queue processing for channel.
     *
     * @param  InputInterface $input
     * @param  OutputInterface $output
     * @return void
     */
    protected function execStopChannel(
        InputInterface $input,
        OutputInterface $output
    ) {
        $channelId = $input->getArgument('id');
        $force = $input->getOption('force');

        if (!$channelId) {
            throw new \RuntimeException("{channel} is required for 'stop-channel' action");
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

        $stopped =
            $this
            ->getService('event_queue.manager')
            ->stopChannel($channelId, $error);

        if (!$stopped) {
            throw new \RuntimeException("Cannot stop '{$channelId}' channel: {$error}");
        }

        $output->writeln("");
        $output->writeln("<options=bold>Stopped '{$channelId}' channel</>");
        $output->writeln("");
    }

    /**
     * Restarts queue processing for channel.
     *
     * @param  InputInterface $input
     * @param  OutputInterface $output
     * @return void
     */
    protected function execRestartChannel(
        InputInterface $input,
        OutputInterface $output
    ) {
        $channelId = $input->getArgument('id');

        if (!$channelId) {
            throw new \RuntimeException("{channel} is required for 'restart-channel' action");
        }

        $restarted =
            $this
            ->getService('event_queue.manager')
            ->restartChannel($channelId, $error);

        if (!$restarted) {
            throw new \RuntimeException("Cannot restart '{$channelId}' channel: {$error}");
        }

        $output->writeln("");
        $output->writeln("<options=bold>Restarted '{$channelId}' channel</>");
        $output->writeln("");
    }

    /**
     * Starts queue processing for all channels.
     *
     * @param  InputInterface $input
     * @param  OutputInterface $output
     * @return void
     */
    protected function execStartAllChannels(
        InputInterface $input,
        OutputInterface $output
    ) {
        $results =
            $this
            ->getService('event_queue.manager')
            ->startAllChannels();

        $this->outputAllChannelStatusChangeResults($output, $results, 'STARTED');
    }

    /**
     * Stops queue processing for all channels.
     *
     * @param  InputInterface $input
     * @param  OutputInterface $output
     * @return void
     */
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

        $results =
            $this
            ->getService('event_queue.manager')
            ->stopAllChannels();

        $this->outputAllChannelStatusChangeResults($output, $results, 'STOPPED');
    }

    /**
     * Restarts queue processing for all channels.
     *
     * @param  InputInterface $input
     * @param  OutputInterface $output
     * @return void
     */
    protected function execRestartAllChannels(
        InputInterface $input,
        OutputInterface $output
    ) {
        $results =
            $this
            ->getService('event_queue.manager')
            ->restartAllChannels();

        $this->outputAllChannelStatusChangeResults($output, $results, 'RESTARTED');
    }

    /**
     * Helper method output results from start/stop/restartAllChannels.
     *
     * @param  OutputInterface $output
     * @param  array $results
     * @param  string $status   Status label to display when successful.
     * @return void
     */
    protected function outputAllChannelStatusChangeResults(
        $output,
        array $results,
        $status
    ) {
        $padLength = 30;

        $output->writeln("");
        $output->writeln("<options=bold>" . str_pad("CHANNEL", $padLength) . "STATUS</>");

        foreach ($results as $channelId => list($succeeded, $error)) {
            $msg = str_pad($channelId, $padLength, '.');

            if ($succeeded) {
                $msg .= "<fg=green>{$status}</>";
            } else {
                $msg .= "<fg=red>{$error}</>";
            }

            $output->writeln($msg);
        }

        $output->writeln("");
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
        try {
            $workerId = $input->getArgument('id');

            if (!$workerId) {
                throw new \RuntimeException("{workerId} is required for 'start-worker' action");
            }

            $worker = $this->createWorker($workerId);
        } catch (\Exception $e) {
            $this->logger()->error((string) $e);
            throw $e;
        }

        // Register a shutdown handler with PHP so fatal errors will gracefully
        // interrupt the worker. It also means the fatal error will get logged
        // within App Log!
        $shutdownHandler = function() use ($worker) {
            $error = error_get_last();

            if ($error) {
                $error = json_encode($error);
                $worker->stop(WorkerEntity::DEACTIVATED_REASON_ERROR, $error);
            }
        };

        register_shutdown_function($shutdownHandler);

        $output->writeln("");
        $output->writeln("<options=bold>Starting Worker ID {$workerId}...</>");
        $output->writeln("<fg=green>To stop, use app:eventqueue stop-worker {$this->getService('site')->getSiteId()} {$workerId}.</>");
        $output->writeln("");

        // Notify the worker of its host and system process id when starting.
        $hostname = gethostname();
        $pid = getmypid();

        try {
            $worker->start($hostname, $pid);
        } catch (\Exception $e) {
            print("\n\nCaught exception {$e}");
            $worker->stop(WorkerEntity::DEACTIVATED_REASON_ERROR, (string) $e);
        }
    }

    /**
     * Stops worker.
     *
     * NOTE: Stopping workers is usually handled by the event queue worker
     * monitor service. It is provided here for debug and edge cases.
     *
     * @param  InputInterface $input
     * @param  OutputInterface $output
     * @return void
     */
    protected function execStopWorker(
        InputInterface $input,
        OutputInterface $output
    ) {
        $workerId = $input->getArgument('id');

        if (!$workerId) {
            throw new \RuntimeException("{workerId} is required for 'stop-worker' action");
        }

        $channel =
            $this
            ->getService('event_queue.manager')
            ->getChannelForWorker($workerId);

        $killed = $channel->killWorker($workerId);

        if (!$killed) {
            throw new \RuntimeException("Could not stop workerId {$workerId}");
        }

        $output->writeln("");
        $output->writeln("<options=bold>Stopped workerId {$workerId}</>");
        $output->writeln("");
    }

    /**
     * Stops all workers for a specific channel.
     *
     * NOTE: Stopping workers is usually handled by the event queue worker
     * monitor service. It is provided here for debug and edge cases.
     *
     * @param  InputInterface $input
     * @param  OutputInterface $output
     * @return void
     */
    protected function execStopAllWorkers(
        InputInterface $input,
        OutputInterface $output
    ) {
        $channelId = $input->getArgument('id');

        if (!$channelId) {
            throw new \RuntimeException("{channel} is required for 'stop-all-workers' action");
        }

        $channel =
            $this
            ->getService('event_queue.manager')
            ->getChannel($channelId);

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
            throw new \RuntimeException("Cannot stop all workers for '{$channelId}' channel: {$error}");
        }

        $output->writeln("");
        $output->writeln("<options=bold>Stopped {$result} worker(s) for '{$channelId}' channel</>");
        $output->writeln("");
    }


    /**
     * Retries failed event.
     * NOTE: If the failed event has a pin key, this will also trigger the worker
     * to retry the skipped siblings.
     *
     * @param  InputInterface $input
     * @param  OutputInterface $output
     * @return void
     */
    protected function execRetryEvent(
        InputInterface $input,
        OutputInterface $output
    ) {
        $queueIdList = $input->getArgument('id');
        $useFreshWorker = $input->getOption('use-fresh-worker');

        if (!$queueIdList) {
            throw new \RuntimeException("{queueId} is required for 'retry-event' action");
        }

        $queueIds = explode(',', $queueIdList);
        $criteria = ['queueId' => $queueIds];

        $queueEntries = $this->repo('EventQueue')->_findBy($criteria);

        if (!$queueEntries) {
            throw new \RuntimeException("No queue entries found for queueId(s) {$queueIdList}");
        }

        $queueIdLabel = str_pad("QUEUE ID", 20);
        $resultLabel = str_pad("RESULT", 40);

        $output->writeln("");
        $output->writeln("<options=bold>{$queueIdLabel}{$resultLabel}</>");
        $output->writeln(str_repeat('-', 60));

        $eventQueueManager = $this->getService('event_queue.manager');

        $isDebugMode =
            $this
            ->getService('service_container')
            ->getParameter('kernel.debug');

        $killedWorkerChannelIds = [];

        foreach ($queueEntries as $queueEntry) {
            $queueId = $queueEntry->getQueueId();

            $output->write(str_pad($queueId, 20));

            if ($queueEntry->getStatus() != EventQueue::STATUS_EXCEPTION_OCCURRED) {
                $result = "<fg=red>Requires (E)xception Status</>";
                $output->writeln($result);
                continue;
            }

            $channelId = $queueEntry->getChannel();
            $channel = $eventQueueManager->getChannel($channelId);

            if ($useFreshWorker && !in_array($channelId, $killedWorkerChannelIds)) {
                $controlLockId = $channel->acquireControlLock();

                if (!$controlLockId) {
                    throw new \RuntimeException("Another process owns the control lock for the '{$channelId}' channel");
                }

                $channel->killAllWorkers($controlLockId);
                $channel->releaseControlLock($controlLockId);

                $killedWorkerChannelIds[] = $channelId;
            }

            $reDispatchedCount = $channel->retryFailedPersistedEvent($queueEntry);

            if ($reDispatchedCount) {
                $result = "<fg=green>Retrying {$reDispatchedCount} event(s)</>";
            } else {
                $result = "<fg=red>Failed to Retry</>";
            }

            $output->writeln($result);
        }

        $output->writeln("");
    }

    /**
     * Re-dispatches event.
     * @param  InputInterface $input
     * @param  OutputInterface $output
     * @return void
     */
    protected function execReDispatchEvent(
        InputInterface $input,
        OutputInterface $output
    ) {
        $queueId = $input->getArgument('id');
        $useFreshWorker = $input->getOption('use-fresh-worker');
        $force = $input->getOption('force');

        if (! $queueId) {
            throw new \RuntimeException("{queueId} is required for 'redispatch-event' action");
        }

        $sourceQueueEntry = $this->repo('EventQueue')->_mustFind($queueId);

        $eventName      = $sourceQueueEntry->getEvent();
        $encodedData    = $sourceQueueEntry->getData();
        $referenceKey   = $sourceQueueEntry->getReferenceKey();
        $pinKey         = $sourceQueueEntry->getPinKey();

        if (!$force) {
            $confirmMsg = "<options=bold>Are you sure you want to re-dispatch '{$eventName}' event (queueId {$queueId})?</>"
                        . "\n<fg=red>WARNING: Doing so may lead to data duplication or errors.</>"
                        . "\n\n<options=bold>Really re-dispatch this event? Y/n</> ";
            $dialog     = $this->getHelperSet()->get('dialog');
            $continue   = $dialog->askConfirmation($output, "\n{$confirmMsg}");

            if (! $continue) {
                $output->writeln("");
                $output->writeln("<options=bold>Event *Not* Re-dispatched</>");
                $output->writeln("");
                return;
            }
        }

        if ($encodedData) {
            $data = json_decode($encodedData, true);
        } else {
            $data = [];
        }

        $isDebugMode = $this
                        ->getService('service_container')
                        ->getParameter('kernel.debug');

        if ($isDebugMode || $useFreshWorker) {
            $channelId = $sourceQueueEntry->getChannel();
            $channel = $this->getService('event_queue.manager')->getChannel($channelId);
            $controlLockId = $channel->acquireControlLock();

            if (!$controlLockId) {
                throw new \RuntimeException('Another process owns this channel\'s control lock');
            }
            $channel->killAllWorkers($controlLockId);
            $channel->releaseControlLock($controlLockId);
        }

        $newQueueEntry =
            $this
            ->getService('event_queue.dispatcher')
            ->dispatch($eventName, $data, $referenceKey, $pinKey);

        $newQueueId = $newQueueEntry->getQueueId();

        $output->writeln("");
        $output->writeln("<options=bold>Re-dispatched '{$eventName}' event (queueId {$queueId})</>");
        $output->writeln("<fg=green>New event saved to queueId {$newQueueId}</>");
        $output->writeln("");
    }

    /**
     * Adds event.
     *
     * @param  InputInterface $input
     * @param  OutputInterface $output
     * @return void
     */
    protected function execDispatchEvent(
        InputInterface $input,
        OutputInterface $output
    ) {
        $eventName = $input->getArgument('id');

        if (! $eventName) {
            throw new \RuntimeException("{eventName} is required for 'dispatch-event' action");
        }

        $referenceKey   = $input->getOption('reference-key');
        $pinKey         = $input->getOption('pin-key');
        $encodedData    = $input->getOption('data');

        if ($encodedData) {
            $data = json_decode($encodedData, true);

            if (is_null($data)) {
                throw new \RuntimeException('$data JSON could not be parsed');
            }
        } else {
            $data = [];
        }

        $queueEntry =
            $this
            ->getService('event_queue.dispatcher')
            ->dispatch($eventName, $data, $referenceKey, $pinKey);

        $queueId = $queueEntry->getQueueId();

        $output->writeln("");
        $output->writeln("<options=bold>Dispatched new '{$eventName}' event</>");
        $output->writeln("<fg=green>Event saved to queueId {$queueId}</>");
        $output->writeln("");
    }

    /**
     * Updates channel's max worker configuration.
     *
     * @param  InputInterface $input
     * @param  OutputInterface $output
     * @return void
     */
    protected function execUpdateChannel(
        InputInterface $input,
        OutputInterface $output
    ) {
        $channelId = $input->getArgument('id');

        if (!$channelId) {
            throw new \RuntimeException("{channel} is required for 'update-channel' action");
        }

        $channel =
            $this
            ->getService('event_queue.manager')
            ->getChannel($channelId);

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

    /**
     * Creates EventQueueWorker.
     * @param integer $workerId
     * @return EventQueueWorker
     * @throws RuntimeException If specified worker is invalid.
     */
    protected function createWorker($workerId)
    {
        $queueManager = $this->getService('event_queue.manager');

        $channel = $queueManager->getChannelForWorker($workerId);

        if (!$channel) {
            throw new \RuntimeException("Invalid workerId {$workerId}");
        }

        $worker =
            new EventQueueWorker(
                $workerId,
                $channel,
                $this->getService('event_queue.processing_manager'),
                $this->getService('event_queue.dispatcher'),
                $queueManager->getEntityManager(),
                $this->getService('doctrine'),
                $this->getService('logger')
            );

        // Get additional worker configuration through service container
        // parameters.
        $container = $this->getService('service_container');

        $maxListenerAttempts =
            $container
            ->getParameter('event_queue.worker_max_listener_attempts');

        $memoryUsagePercentage =
            $container
            ->getParameter('event_queue.worker_memory_usage_percentage');

        $sleepSeconds =
            $container
            ->getParameter('event_queue.worker_sleep_seconds');

        if ($maxListenerAttempts) {
            $worker->setMaxListenerAttempts($maxListenerAttempts);
        }

        if ($memoryUsagePercentage) {
            $worker->setMemoryUsagePercentage($memoryUsagePercentage);
        }

        if ($sleepSeconds) {
            $worker->setSleepSeconds($sleepSeconds);
        }

        return $worker;
    }

}
