<?php
namespace Celltrak\EventQueueBundle\DependencyInjection;

use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Compiler\CompilerPassInterface;
use Symfony\Component\DependencyInjection\Reference;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;
use CTLib\Util\DependencySorter;
use CTLib\Util\Util;

/**
 * Compiler pass for event queue services.
 *
 * @author Mike Turoff
 */
class CelltrakEventQueueCompilerPass implements CompilerPassInterface
{

    /**
     * {@inheritDoct}
     */
    public function process(ContainerBuilder $container)
    {
        $serviceId = 'event_queue.processing_manager';

        if (!$container->hasDefinition($serviceId)) {
            return;
        }

        $this->container = $container;
        $this->processingManagerDefinition = $container->getDefinition($serviceId);
        $this->registeredEvents = $container->getParameter('event_queue.events');

        $this->registerListeners();
        $this->registerPreProcessors();
        $this->registerPostProcessors();
    }

    /**
     * Registers tagged event listeners.
     * @param  Container $container
     * @param  Definition $definition
     * @return void
     */
    protected function registerListeners()
    {
        $tag        = 'app.event_listener';
        $services   = $this->container->findTaggedServiceIds($tag);

        if (!$services) {
            return;
        }

        $listeners  = $this->compileEventListeners($services);
        $sorter     = $this->getEventListenerDependencySorter();

        foreach ($listeners as $event => $eventListeners) {
            $sortedEventListeners = $sorter->sort($eventListeners);

            foreach ($sortedEventListeners as $listener) {
                $args = [
                    $event,
                    $listener->listenerId,
                    $listener->serviceId,
                    $listener->method,
                    $listener->requires
                ];

                $this
                ->processingManagerDefinition
                ->addMethodCall('registerListener', $args);
            }
        }
    }

    /**
     * Registers tagged event pre-processors.
     * @param  Container $container
     * @param  Definition $definition
     * @return void
     */
    protected function registerPreProcessors()
    {
        $tag        = 'app.event_pre_processor';
        $services   = $this->container->findTaggedServiceIds($tag);

        if (!$services) {
            return;
        }

        foreach ($services as $serviceId => $tagAttributes) {
            foreach ($tagAttributes as $attributes) {
                $event = $attributes['event'];

                if (!$this->isRegisteredEvent($event)) {
                    throw new InvalidConfigurationException("Service '{$serviceId}' is registering as a pre-processor for the '{$event}' event, which is not registered to be handled by any channel");
                }

                $args = [$event, $serviceId];

                $this
                ->processingManagerDefinition
                ->addMethodCall('registerPreProcessor', $args);
            }
        }
    }

    /**
     * Registers tagged event post-processors.
     * @param  Container $container
     * @param  Definition $definition
     * @return void
     */
    protected function registerPostProcessors()
    {
        $tag        = 'app.event_post_processor';
        $services   = $this->container->findTaggedServiceIds($tag);

        if (!$services) {
            return;
        }

        foreach ($services as $serviceId => $tagAttributes) {
            foreach ($tagAttributes as $attributes) {
                $event = $attributes['event'];

                if (!$this->isRegisteredEvent($event)) {
                    throw new InvalidConfigurationException("Service '{$serviceId}' is registering as a post-processor for the '{$event}' event, which is not registered to be handled by any channel");
                }

                $args = [$event, $serviceId];

                $this
                ->processingManagerDefinition
                ->addMethodCall('registerPostProcessor', $args);
            }
        }
    }

    /**
     * Compiles tagged event listener services into elaborated structure
     * required to properly sort them based on their pre-requisites.
     * @param  array $services
     * @return array           [$event => [listener stdClass]]
     */
    protected function compileEventListeners($services)
    {
        $listeners      = [];
        $listenerIds    = [];

        // First iterate through the tagged services building a standard array
        // of listeners (per event) along with the complete list of listener ids
        // (per event).
        foreach ($services as $serviceId => $tagAttributes) {
            foreach ($tagAttributes as $attributes) {
                $event = $attributes['event'];

                if (!$this->isRegisteredEvent($event)) {
                    throw new InvalidConfigurationException("Service '{$serviceId}' listens for the '{$event}' event, which is not registered to be handled by any channel");
                }

                if (isset($attributes['method'])) {
                    $method = $attributes['method'];
                } else {
                    $method = $this->getListenerMethodForEvent($event);
                }

                $listenerId = "{$serviceId}#{$method}";
                $listenerId = strtolower($listenerId);

                if (isset($attributes['requires'])) {
                    $requires = explode(',', $attributes['requires']);
                    $requires = array_map('trim', $requires);
                    $requires = array_map('strtolower', $requires);
                } else {
                    $requires = [];
                }

                $listener = new \stdClass;
                $listener->serviceId    = $serviceId;
                $listener->listenerId   = $listenerId;
                $listener->method       = $method;
                $listener->requires     = $requires;

                $listeners[$event][]    = $listener;
                $listenerIds[$event][]  = $listenerId;
            }
        }

        // Now iterate through the listeners to dynamically expand shortcut
        // required listener ids. The following shortcuts are supported:
        //      *               Indicates listener requires ALL of its siblings.
        //      service_id      Indicates listener requires all listeners from
        //                      specified service_id.
        //
        // The standard required listener id takes the format of
        //
        //      service_id#callback_method
        //
        //      For example, activity_add_filters#onActivityStart
        //
        foreach ($listeners as $event => $eventListeners) {
            $eventListenerIds = $listenerIds[$event];

            foreach ($eventListeners as $listener) {
                $listenerId         = $listener->listenerId;
                $requires           = $listener->requires;
                $expandedRequires   = [];

                foreach ($requires as $requiredListenerId) {
                    if ($requiredListenerId == '*') {
                        // Listener requires all sibling listeners.
                        $requiredListenerIds = array_diff(
                                                $eventListenerIds,
                                                [$listenerId]);

                        $expandedRequires = array_merge(
                                                $expandedRequires,
                                                $requiredListenerIds);
                    } elseif (strpos($requiredListenerId, '#') === false) {
                        // Listener requires all sibling listeners from a single
                        // service.
                        $requiredListenerIds = array_filter(
                            $eventListenerIds,
                            function($id) use ($requiredListenerId) {
                                $listenerIdTokens   = explode('#', $id);
                                $serviceId          = $listenerIdTokens[0];
                                return $serviceId == $requiredListenerId;
                            });

                        $expandedRequires = array_merge(
                                                $expandedRequires,
                                                $requiredListenerIds);
                    } else {
                        // Listener requires explicit listenerId. Include as is.
                        $expandedRequires[] = $requiredListenerId;
                    }
                }

                $listener->requires = array_unique($expandedRequires);
            }
        }

        return $listeners;
    }

    /**
     * Convert event to callback method.
     * For example, activity.start will become onactivitystart.
     * @param  string $event Event name.
     * @return string        Callback method.
     */
    protected function getListenerMethodForEvent($event)
    {
        return "on" . str_replace(['.', '_'], '', $event);
    }

    /**
     * Creates DependencySorter to prioritize event listeners.
     * @return DependencySorter
     */
    protected function getEventListenerDependencySorter()
    {
        $getRequiresCallback =
            function($listener) {
                return $listener->requires;
            };

        $isRequiredCallback =
            function($listener, $requiredId) {
                $listenerId = $listener->listenerId;
                return $requiredId == $listenerId;
            };

        return new DependencySorter($getRequiresCallback, $isRequiredCallback);
    }

    /**
     * Indicates whether event is registered to a queue channel.
     * @param string $event
     * @return boolean
     */
    protected function isRegisteredEvent($event)
    {
        return in_array($event, $this->registeredEvents);
    }

}
