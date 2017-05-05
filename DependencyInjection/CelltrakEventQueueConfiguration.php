<?php
namespace Celltrak\EventQueueBundle\DependencyInjection;

use Symfony\Component\Config\Definition\ConfigurationInterface;
use Symfony\Component\Config\Definition\Builder\TreeBuilder;

/**
 * EventQueue dependency injection extension configuration in preparation for
 * an eventual separation into its own bundle.
 *
 * @author Mike Turoff
 */
class CelltrakEventQueueConfiguration implements ConfigurationInterface
{

    public function getConfigTreeBuilder()
    {
        $tb = new TreeBuilder;
        $root = $tb->root('celltrak_event_queue');

        $root
            ->children()
                ->scalarNode('redis_client')
                    ->info('The service ID for the CelltrakRedis client used by the event queue')
                    ->isRequired()
                    ->cannotBeEmpty()
                ->end()
                ->scalarNode('entity_manager')
                    ->info('The service ID for the Doctrine EntityManager used by the event queue')
                    ->isRequired()
                    ->cannotBeEmpty()
                ->end()
                ->scalarNode('api_auth_key')
                    ->info('The secret key required to call the event queue REST API (use NULL to disable request authorization)')
                    ->isRequired()
                ->end()
                ->scalarNode('api_auth_algorithm')
                    ->info('The hashing algorithm used to sign event queue REST API requests (defaults to sha1)')
                    ->defaultValue('sha1')
                ->end()
                ->scalarNode('tenant_key')
                    ->info('The token used to run the event queue in multi-tenant mode (defaults to single tenant)')
                    ->defaultNull()
                ->end()
                ->integerNode('worker_provision_timeout')
                    ->info('The maximum number of seconds to wait during worker provisioning (defaults to 30)')
                    ->min(1)
                    ->defaultValue(30)
                ->end()
                ->floatNode('worker_memory_usage_percentage')
                    ->info('The percentage of the PHP memory_limit an individual worker can consume (defaults to .9)')
                    ->min(.1)->max(.99)
                    ->defaultValue(.9)
                ->end()
                ->integerNode('worker_sleep_seconds')
                    ->info('The number of seconds a worker will sleep when waiting for more events (defaults to 3)')
                    ->min(0)
                    ->defaultValue(3)
                ->end()
                ->integerNode('max_listener_attempts')
                    ->info('The maximum attempts to call an individual listener if failing due to transaction deadlock (defaults to 3)')
                    ->min(1)
                    ->defaultValue(3)
                ->end()
                ->scalarNode('symfony_command_executor_factory')
                    ->info('Service used to create CommandExecutor required to fork worker process')
                    ->defaultValue('symfony_command_executor_factory')
                ->end()
                ->integerNode('database_record_ttl_days')
                    ->info('The maximum number of days for event queue database records to live before being garbage collected')
                    ->min(0)
                    ->defaultValue(5)
                ->end()
                ->arrayNode('channels')
                    ->info('Define each channel')
                    ->useAttributeAsKey('channelId')
                    ->isRequired()
                    ->requiresAtLeastOneElement()
                    ->prototype('array')
                        ->children()
                            ->integerNode('default_max_workers')
                                ->info('The default maximum number of workers allowed to run concurrently for this channel')
                                ->min(1)
                                ->isRequired()
                            ->end()
                            ->integerNode('default_max_load')
                                ->info('The default maximum number of events per worker before queue attempts to add another worker')
                                ->min(1)
                                ->isRequired()
                            ->end()
                            ->integerNode('worker_zombie_idle_seconds')
                                ->info('The number of seconds since a worker\'s last check-in before it\'s considered a zombie (defaults to 300)')
                                ->min(30)
                                ->defaultValue(300)
                            ->end()
                            ->arrayNode('events')
                                ->info('Events handled by this channel')
                                ->isRequired()
                                ->requiresAtLeastOneElement()
                                ->prototype('scalar')
                                ->end()
                            ->end()
                        ->end()
                    ->end()
                ->end()
            ->end()
        ->end();

        return $tb;
    }

}
