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
use Celltrak\EventQueueBundle\Entity\EventQueue;
use CTLib\Component\Monolog\Logger;
use CTLib\Component\Console\ConsoleOutputHelper;
use CTLib\Component\Console\ConsoleProcessResult;


/**
 * Works with event queue events.
 * @author Mike Turoff
 */
class EventQueueEventCommand extends ContainerAwareCommand
{

    /**
     * {@inheritDoc}
     */
    protected function configure()
    {
        $this
            ->setName('event_queue:event')
            ->setDescription('Works with event queue events')
            ->addArgument('action', InputArgument::REQUIRED, "Use 'list' to see available actions")
            ->addArgument('queueIdOrEventName', InputArgument::REQUIRED, 'Either EventQueue.queueId or event name depending on action')
            ->addOption('data', null, InputOption::VALUE_REQUIRED, 'JSON-encoded data when dispatching event')
            ->addOption('reference-key', null, InputOption::VALUE_REQUIRED, 'Reference Key when dispatching event')
            ->addOption('pin-key', null, InputOption::VALUE_REQUIRED, 'Pin Key when dispatching event')
            ->addOption('use-fresh-worker', null, InputOption::VALUE_NONE, 'Forces creation of fresh workers when dispatching/retrying events')
            ->addOption('force', 'f', InputOption::VALUE_NONE, 'Skip confirmation prompts');
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

            case 'retry':
                return $this->execRetryEvent($input, $output);
            case 'redispatch':
                return $this->execReDispatchEvent($input, $output);
            case 'dispatch':
                return $this->execDispatchEvent($input, $output);

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
        $this->entityManager = $this->eventQueueManager->getEntityManager();
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
            'retry {queueId,...}' => 'Retries failed event(s)',
            'redispatch {queueId}' => 'Re-dispatches copy of event',
            'dispatch {eventName}' => 'Dispatches new event'
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
        $queueIdList = $input->getArgument('queueIdOrEventName');
        $useFreshWorker = $input->getOption('use-fresh-worker');

        $queueIds = array_map('trim', explode(',', $queueIdList));
        $criteria = ['queueId' => $queueIds];

        $queueEntries = $this->entityManager
            ->getRepository('CelltrakEventQueueBundle:EventQueue')
            ->_findBy($criteria);

        if (!$queueEntries) {
            throw new \RuntimeException("No queue entries found for queueId(s) {$queueIdList}");
        }

        $output->writeln("");

        $outputHelper = new ConsoleOutputHelper($output);
        $killedWorkerChannelIds = [];

        foreach ($queueEntries as $queueEntry) {
            $queueId = $queueEntry->getQueueId();

            $result = new ConsoleProcessResult($queueId);

            if ($queueEntry->getStatus() != EventQueue::STATUS_EXCEPTION_OCCURRED) {
                $result->failure("Requires (E)xception Status");
            } else {
                $channelId = $queueEntry->getChannel();
                $channel = $this->eventQueueManager->getChannel($channelId);

                if ($useFreshWorker
                    && !in_array($channelId, $killedWorkerChannelIds)
                ) {
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
                    $result->success("Retrying {$reDispatchedCount} event(s)");
                } else {
                    $result->failure("Failed to Retry");
                }
            }

            $outputHelper->outputProcessResult($result, 20);
        }

        $output->writeln("");
    }

    /**
     * Re-dispatches copy of existing event.
     * @param  InputInterface $input
     * @param  OutputInterface $output
     * @return void
     */
    protected function execReDispatchEvent(
        InputInterface $input,
        OutputInterface $output
    ) {
        $queueId = $input->getArgument('queueIdOrEventName');
        $useFreshWorker = $input->getOption('use-fresh-worker');
        $force = $input->getOption('force');

        $sourceQueueEntry = $this->entityManager
            ->getRepository('CelltrakEventQueueBundle:EventQueue')
            ->_mustFind($queueId);

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

        if ($this->isDebug || $useFreshWorker) {
            $channelId = $sourceQueueEntry->getChannel();
            $channel = $this->eventQueueManager->getChannel($channelId);
            $controlLockId = $channel->acquireControlLock();

            if (!$controlLockId) {
                throw new \RuntimeException('Another process owns this channel\'s control lock');
            }
            $channel->killAllWorkers($controlLockId);
            $channel->releaseControlLock($controlLockId);
        }

        $newQueueEntry =
            $this
            ->eventQueueDispatcher
            ->dispatch($eventName, $data, $referenceKey, $pinKey);

        $newQueueId = $newQueueEntry->getQueueId();

        $output->writeln("");
        $output->writeln("<options=bold>Re-dispatched '{$eventName}' event (queueId {$queueId})</>");
        $output->writeln("<fg=green>New event saved to queueId {$newQueueId}</>");
        $output->writeln("");
    }

    /**
     * Dispatches new event.
     *
     * @param  InputInterface $input
     * @param  OutputInterface $output
     * @return void
     */
    protected function execDispatchEvent(
        InputInterface $input,
        OutputInterface $output
    ) {
        $eventName      = $input->getArgument('queueIdOrEventName');
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
            ->eventQueueDispatcher
            ->dispatch($eventName, $data, $referenceKey, $pinKey);

        $queueId = $queueEntry->getQueueId();

        $output->writeln("");
        $output->writeln("<options=bold>Dispatched new '{$eventName}' event</>");
        $output->writeln("<fg=green>Event saved to queueId {$queueId}</>");
        $output->writeln("");
    }

}
