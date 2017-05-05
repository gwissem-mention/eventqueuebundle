<?php
namespace Celltrak\EventQueueBundle\DependencyInjection;

use Symfony\Component\HttpKernel\DependencyInjection\Extension;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\Config\Definition\Processor;
use Symfony\Component\DependencyInjection\Definition;
use Symfony\Component\DependencyInjection\Reference;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

/**
 * Dependency injection extension for Event Queue in preparation for eventual
 * separation into its own bundle.
 *
 * @author Mike Turoff
 */
class CelltrakEventQueueExtension extends Extension
{

    /**
     * Class namespace for EventQueue service classes.
     */
    const NS = 'Celltrak\EventQueueBundle';

    /**
     * Service IDs.
     */
    const SERVICE_ID_QUEUE_MANAGER      = 'event_queue.manager';
    const SERVICE_ID_DISPATCHER         = 'event_queue.dispatcher';
    const SERVICE_ID_PROCESSING_MANAGER = 'event_queue.processing_manager';
    const SERVICE_ID_WORKER_FACTORY     = 'event_queue.worker_factory';


    /**
     * Loads Event Queue services into Container.
     *
     * @param array $config
     * @param ContainerBuilder $container
     * @return void
     */
    public function load(array $configs, ContainerBuilder $container)
    {
        $processor = new Processor;
        $config = $processor->processConfiguration(
            new CelltrakEventQueueConfiguration,
            $configs
        );


        // Store for use in all the loader methods.
        $this->config = $config;
        $this->container = $container;

        $channelServiceIds = $this->loadEventQueueChannels();
        $this->loadEventQueueManager($channelServiceIds);
        $this->loadProcessingManager();
        $this->loadDispatcher();
        $this->loadApiAuthenticator();
        $this->loadRouteLoader();
        $this->loadWorkerController();
        $this->loadChannelController();
        $this->loadEventsController();
        $this->loadWorkerFactory();
        $this->loadGarbageCollector();
    }

    /**
     * Loads each EventQueueChannel definition.
     *  >> Channels are the primary workhorse of the event queue.
     * @return void
     */
    protected function loadEventQueueChannels()
    {
        $class = self::NS . "\Component\EventQueueChannel";

        $redisClientServiceId   = $this->config['redis_client'];
        $entityManagerServiceId = $this->config['entity_manager'];
        $tenantKey              = $this->config['tenant_key'];

        $redisReference         = new Reference($redisClientServiceId);
        $entityManagerReference = new Reference($entityManagerServiceId);
        $loggerReference        = new Reference('logger');

        $channelServiceIds = [];

        // Track handled events to make sure none are repeated in multiple
        // channels.
        $handledEvents = [];

        foreach ($this->config['channels'] as $channelId => $channel) {
            $channelEvents = $channel['events'];
            $existingEvents = array_intersect($handledEvents, $channelEvents);

            if ($existingEvents) {
                throw new InvalidConfigurationException(join(', ', $existingEvents) . " event(s) are already handled by another channel");
            }

            $serviceId = "event_queue.{$channelId}.channel";

            $args = [
                $channelId,
                $channel['events'],
                $channel['default_max_workers'],
                $channel['default_max_load'],
                $channel['worker_zombie_idle_seconds'],
                $redisReference,
                $entityManagerReference,
                $loggerReference,
                $tenantKey
            ];
            $def = new Definition($class, $args);
            $def->setPublic(false);
            $this->container->setDefinition($serviceId, $def);

            $channelServiceIds[] = $serviceId;

            $handledEvents = array_merge($handledEvents, $channelEvents);
        }

        // Store handled events as container parameter so it can be used in
        // EventQueueCompilerPass.
        $this->container->setParameter('event_queue.events', $handledEvents);

        return $channelServiceIds;
    }

    /**
     * Loads EventQueueManager definition.
     *  >> Controls start/stop of entire queue along with access to individual
     *  >> queue channels.
     * @return void
     */
    protected function loadEventQueueManager(array $channelServiceIds)
    {
        $serviceId = self::SERVICE_ID_QUEUE_MANAGER;

        $class = self::NS . "\Component\EventQueueManager";

        $redisClientServiceId = $this->config['redis_client'];
        $entityManagerServiceId = $this->config['entity_manager'];

        $args = [
            new Reference($redisClientServiceId),
            new Reference($entityManagerServiceId),
            new Reference('logger')
        ];

        $def = new Definition($class, $args);

        // Register channel configuration with EventQueueManager.
        foreach ($channelServiceIds as $channelServiceId) {
            $args = [new Reference($channelServiceId)];
            $def->addMethodCall('registerChannel', $args);
        }

        $this->container->setDefinition($serviceId, $def);

        // For backwards compatability.
        $this->container->setAlias('event_queue', $serviceId);
    }

    /**
     * Loads EventQueueProcessingManager definition.
     *  >> Manages listener service injection into worker.
     * @return void
     */
    protected function loadProcessingManager()
    {
        $serviceId = self::SERVICE_ID_PROCESSING_MANAGER;

        $class = self::NS . "\Component\EventQueueProcessingManager";

        $args = [
            new Reference('service_container')
        ];

        $def = new Definition($class, $args);
        $this->container->setDefinition($serviceId, $def);
    }

    /**
     * Loads EventQueueDispatcher definition.
     *  >> Adds events into the queue.
     * @return void
     */
    protected function loadDispatcher()
    {
        $serviceId = self::SERVICE_ID_DISPATCHER;

        $class = self::NS . "\Component\EventQueueDispatcher";

        $entityManagerServiceId = $this->config['entity_manager'];

        $args = [
            new Reference(self::SERVICE_ID_QUEUE_MANAGER),
            new Reference($entityManagerServiceId),
            new Reference('doctrine'),
            new Reference('logger')
        ];

        $def = new Definition($class, $args);
        $this->container->setDefinition($serviceId, $def);

        // For backwards compatiblity.
        $this->container->setAlias('event_queue_dispatcher', $serviceId);
    }

    /**
     * Loads EventQueueRestApiAuthenticator definition.
     *  >> Verifies that event queue REST API requests are authenticated.
     * @return void
     */
    protected function loadApiAuthenticator()
    {
        $serviceId = 'event_queue.rest_api_authenticator';

        $class = self::NS . "\Component\EventQueueRestApiAuthenticator";

        $apiAuthKey         = $this->config['api_auth_key'];
        $apiAuthAlgorithm   = $this->config['api_auth_algorithm'];

        $args = [
            $apiAuthKey,
            $apiAuthAlgorithm,
            new Reference('logger')
        ];

        $def = new Definition($class, $args);
        $def->addTag('ctlib.web_service_request_authenticator');
        $def->setPublic(false);
        $this->container->setDefinition($serviceId, $def);
    }

    /**
     * Loads EventQueueRouteLoader definition.
     *  >> Adds event queue REST API routes into configuration.
     * @return void
     */
    protected function loadRouteLoader()
    {
        $serviceId = 'event_queue.route_loader';

        $class = self::NS . "\Routing\EventQueueRouteLoader";

        $def = new Definition($class);
        $def->addTag('routing.loader');
        $def->setPublic(false);
        $this->container->setDefinition($serviceId, $def);
    }

    /**
     * Loads EventQueueWorkerController definition.
     *  >> Serves event queue worker REST API.
     * @return void
     */
    protected function loadWorkerController()
    {
        $serviceId = 'event_queue.worker_controller';

        $class = self::NS . "\Controller\EventQueueWorkerController";

        $entityManagerServiceId = $this->config['entity_manager'];
        $commandExecutorFactoryServiceId = $this->config['symfony_command_executor_factory'];

        $args = [
            new Reference(self::SERVICE_ID_QUEUE_MANAGER),
            new Reference($commandExecutorFactoryServiceId),
            new Reference($entityManagerServiceId),
            new Reference('logger'),
            $this->config['worker_provision_timeout']
        ];

        $def = new Definition($class, $args);
        $this->container->setDefinition($serviceId, $def);
    }

    /**
     * Loads EventQueueChannelController definition.
     *  >> Serves event queue channel REST API.
     * @return void
     */
    protected function loadChannelController()
    {
        $serviceId = 'event_queue.channel_controller';

        $class = self::NS . "\Controller\EventQueueChannelController";

        $args = [
            new Reference('logger')
        ];

        $def = new Definition($class, $args);
        $this->container->setDefinition($serviceId, $def);
    }

    /**
     * Loads EventQueueEventsController definition.
     *  >> Serves event queue Events REST API.
     * @return void
     */
    protected function loadEventsController()
    {
        $serviceId = 'event_queue.events_controller';

        $class = self::NS . "\Controller\EventQueueEventsController";

        $args = [
            new Reference(self::SERVICE_ID_DISPATCHER),
            new Reference('logger')
        ];

        $def = new Definition($class, $args);
        $this->container->setDefinition($serviceId, $def);
    }

    /**
     * Loads EventQueueWorkerFactory definition.
     * @return void
     */
    protected function loadWorkerFactory()
    {
        $serviceId = self::SERVICE_ID_WORKER_FACTORY;

        $class = self::NS . "\Component\EventQueueWorkerFactory";

        $entityManagerServiceId = $this->config['entity_manager'];

        $args = [
            new Reference(self::SERVICE_ID_QUEUE_MANAGER),
            new Reference(self::SERVICE_ID_PROCESSING_MANAGER),
            new Reference(self::SERVICE_ID_DISPATCHER),
            new Reference($entityManagerServiceId),
            new Reference('doctrine'),
            new Reference('logger'),
            $this->config['max_listener_attempts'],
            $this->config['worker_memory_usage_percentage'],
            $this->config['worker_sleep_seconds']
        ];

        $def = new Definition($class, $args);
        $this->container->setDefinition($serviceId, $def);
    }

    /**
     * Loads EventQueueGarbageCollector definition.
     * @return void
     */
    protected function loadGarbageCollector()
    {
        $serviceId = "event_queue.garbage_collector";

        $class = self::NS . "\Component\EventQueueGarbageCollector";

        $entityManagerServiceId = $this->config['entity_manager'];
        $ttlDays = $this->config['database_record_ttl_days'];

        $args = [
            $ttlDays,
            new Reference($entityManagerServiceId),
            new Reference('logger')
        ];

        $def = new Definition($class, $args);
        $def->addTag('ctlib.garbage_collector');
        $this->container->setDefinition($serviceId, $def);
    }

}
