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
     * Service ID for EventQueueManager service.
     */
    const QUEUE_MANAGER_SERVICE_ID = 'event_queue.manager';


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
            new CellTrakEventQueueConfiguration,
            $configs
        );


        // Store for use in all the loader methods.
        $this->config = $config;
        $this->container = $container;

        $this->setWorkerParameters();
        $this->loadEventQueueManager();
        $this->loadProcessingManager();
        $this->loadDispatcher();
        // $this->loadApiAuthenticator();
        $this->loadRouteLoader();
        $this->loadWorkerController();
        $this->loadChannelController();
    }

    /**
     * Creates container parameters required when initializing a new worker.
     * @return void
     */
    protected function setWorkerParameters()
    {
        $this
        ->container
        ->setParameter(
            'event_queue.worker_max_listener_attempts',
            $this->config['max_listener_attempts']
        );

        $this
        ->container
        ->setParameter(
            'event_queue.worker_memory_usage_percentage',
            $this->config['worker_memory_usage_percentage']
        );

        $this
        ->container
        ->setParameter(
            'event_queue.worker_sleep_seconds',
            $this->config['worker_sleep_seconds']
        );
    }

    /**
     * Loads EventQueueManager definition.
     *  >> Controls start/stop of entire queue along with access to individual
     *  >> queue channels.
     * @return void
     */
    protected function loadEventQueueManager()
    {
        $serviceId = self::QUEUE_MANAGER_SERVICE_ID;

        $class = self::NS . "\Component\EventQueueManager";

        $redisClientServiceId = $this->config['redis_client'];
        $entityManagerServiceId = $this->config['entity_manager'];

        $args = [
            new Reference($redisClientServiceId),
            new Reference($entityManagerServiceId),
            new Reference('logger')
        ];

        $def = new Definition($class, $args);
        $tagAttributes = ['purgeThreshold' => 'gc.eventqueue_purge_age'];
        $def->addTag('app.garbage_collector', $tagAttributes);

        $this->container->setDefinition($serviceId, $def);

        // For backwards compatability.
        $this->container->setAlias('event_queue', $serviceId);

        // Optional settings.
        $tenantKey = $this->config['tenant_key'];
        $zombieWorkerIdleSeconds = $this->config['worker_to_zombie_idle_seconds'];

        if ($tenantKey) {
            $def->addMethodCall('setTenantKey', [$tenantKey]);
        }

        if ($zombieWorkerIdleSeconds) {
            $def
            ->addMethodCall(
                'setZombieWorkerIdleSeconds',
                [$zombieWorkerIdleSeconds]
            );
        }

        // Register channel configuration with EventQueueManager.
        $this->registerChannels($def, $this->config['channels']);
    }

    /**
     * Registers channel configuration with EventQueueManager.
     * @param Definition $managerDefinition
     * @param array $channels
     * @return void
     * @throws InvalidConfigurationException If more than one channel configured
     *                                       to handle same event.
     */
    protected function registerChannels(
        Definition $managerDefinition,
        array $channels
    ) {
        // Track handled events to make sure none are repeated in multiple
        // channels.
        $handledEvents = [];

        foreach ($channels as $channelId => $channel) {
            $channelEvents = $channel['events'];
            $existingEvents = array_intersect($handledEvents, $channelEvents);

            if ($existingEvents) {
                throw new InvalidConfigurationException(join(', ', $existingEvents) . " events are already handled by another channel");
            }

            $args = [
                $channelId,
                $channel['events'],
                $channel['default_max_workers'],
                $channel['default_max_load']
            ];
            $managerDefinition->addMethodCall('registerChannel', $args);

            $handledEvents = array_merge($handledEvents, $channelEvents);
        }

        // Store handled events as container parameter so it can be used in
        // EventQueueCompilerPass.
        $this->container->setParameter('event_queue.events', $handledEvents);
    }

    /**
     * Loads EventQueueProcessingManager definition.
     *  >> Manages listener service injection into worker.
     * @return void
     */
    protected function loadProcessingManager()
    {
        $serviceId = 'event_queue.processing_manager';

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
        $serviceId = 'event_queue.dispatcher';

        $class = self::NS . "\Component\EventQueueDispatcher";

        $entityManagerServiceId = $this->config['entity_manager'];

        $args = [
            new Reference(self::QUEUE_MANAGER_SERVICE_ID),
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

        $class = self::NS . "\Security\EventQueueRestApiAuthenticator";

        $apiAuthKey         = $this->config['api_auth_key'];
        $apiAuthAlgorithm   = $this->config['api_auth_algorithm'];

        $args = [
            $apiAuthKey,
            $apiAuthAlgorithm,
            new Reference('logger')
        ];

        $def = new Definition($class, $args);
        $def->addTag('app.web_service_request_authenticator');
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

        $args = [
            new Reference(self::QUEUE_MANAGER_SERVICE_ID),
            new Reference('site'),
            new Reference($entityManagerServiceId),
            new Reference('logger')
        ];

        $def = new Definition($class, $args);
        $this->container->setDefinition($serviceId, $def);

        $workerProvisionTimeout = $this->config['worker_provision_timeout'];

        if ($workerProvisionTimeout) {
            $def
            ->addMethodCall(
                'setWorkerProvisionTimeout',
                [$workerProvisionTimeout]
            );
        }
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

}
