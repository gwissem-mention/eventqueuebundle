<?php
namespace Celltrak\EventQueueBundle\Component;

use Symfony\Component\DependencyInjection\ContainerInterface;


/**
 * Service container for channel's registered listeners, pre-processors, and
 * post-processors.
 *
 * @author Mike Turoff
 */
class EventQueueProcessingManager
{

    /**
     * @var ContainerInterface
     */
    protected $serviceContainer;

    /**
     * @var Logger
     */
     protected $logger;

     /**
      * Set of event listener configurations.
      * @var array
      */
     protected $listenerConfigs;

     /**
      * Set of compiled listeners ready to be handed off to the worker.
      * @var array
      */
     protected $listeners;

     /**
      * Set of event pre-processor service IDs.
      * @var array
      */
     protected $preProcessorServiceIds;

     /**
      * Set of compiled pre-processors ready to be handed off to the worker.
      * @var array
      */
     protected $preProcessors;

     /**
      * Set of event post-processor service IDs.
      * @var array
      */
     protected $postProcessorServiceIds;

     /**
      * Set of compiled post-processors ready to be handed off to the worker.
      * @var array
      */
     protected $postProcessors;

    /**
     * @param ContainerInterface $serviceContainer
     */
    public function __construct(ContainerInterface $serviceContainer) {
        $this->serviceContainer         = $serviceContainer;
        $this->logger                   = $serviceContainer->get('logger');
        $this->listenerConfigs          = [];
        $this->listeners                = [];
        $this->preProcessorServiceIds   = [];
        $this->preProcessors            = [];
        $this->postProcessorServiceIds  = [];
        $this->postProcessors           = [];
    }

    /**
     * Registers event listener.
     * @param  string $eventName
     * @param  string $listenerId Unique identifier for listener.
     * @param  string $serviceId
     * @param  string $method     Callback method name.
     * @param  array  $requires   Set of pre-requisite $listenerIds.
     * @return void
     */
    public function registerListener(
        $eventName,
        $listenerId,
        $serviceId,
        $method,
        array $requires = []
    ) {
        $listenerConfig             = new \stdClass;
        $listenerConfig->id         = $listenerId;
        $listenerConfig->serviceId  = $serviceId;
        $listenerConfig->method     = $method;
        $listenerConfig->requires   = $requires;

        $this->listenerConfigs[$eventName][] = $listenerConfig;

        // Clear out compiled listeners for this event.
        unset($this->listeners[$eventName]);
    }

    /**
     * Registers event pre-processor.
     * @param  string $eventName
     * @param  string $serviceId
     * @return void
     */
    public function registerPreProcessor($eventName, $serviceId)
    {
        if (!isset($this->listenerConfigs[$eventName])) {
            throw new \RuntimeException("Cannot register pre-processor for '{$eventName}' event because it does not have any registered listeners");
        }

        $this->preProcessorServiceIds[$eventName][] = $serviceId;

        // Clear out compiled pre-processors for this event.
        unset($this->preProcessors[$eventName]);
    }

    /**
     * Registers event post-processor.
     * @param  string $eventName
     * @param  string $serviceId
     * @return void
     */
    public function registerPostProcessor($eventName, $serviceId)
    {
        if (!isset($this->listenerConfigs[$eventName])) {
            throw new \RuntimeException("Cannot register post-processor for '{$eventName}' event because it does not have any registered listeners");
        }

        $this->postProcessorServiceIds[$eventName][] = $serviceId;

        // Clear out compiled post-processors for this event.
        unset($this->postProcessors[$eventName]);
    }

    /**
     * Indicates whether event has any registered listeners.
     * @param  string  $eventName
     * @return boolean
     */
    public function hasListenersForEvent($eventName)
    {
        return isset($this->listenerConfigs[$eventName]);
    }

    /**
     * Returns listener ids for event.
     * @param string $eventName
     * @return array
     */
    public function getListenerIdsForEvent($eventName)
    {
        if (!isset($this->listenerConfigs[$eventName])) {
            return [];
        }

        return array_map(
            function($listener) { return $listener->id; },
            $this->listenerConfigs[$eventName]
        );
    }

    /**
     * Returns listeners registered for event.
     * @param  string $eventName
     * @return array
     */
    public function getListenersForEvent($eventName)
    {
        if (!isset($this->listeners[$eventName])) {
            $this->compileListenersForEvent($eventName);
        }

        return $this->listeners[$eventName];
    }

    /**
     * Returns pre-processors registered for event.
     * @param  string $eventName
     * @return array
     */
    public function getPreProcessorsForEvent($eventName)
    {
        if (!isset($this->preProcessors[$eventName])) {
            $this->compilePreProcessorsForEvent($eventName);
        }

        return $this->preProcessors[$eventName];
    }

    /**
     * Returns post-processors registered for event.
     * @param  string $eventName
     * @return array
     */
    public function getPostProcessorsForEvent($eventName)
    {
        if (!isset($this->postProcessors[$eventName])) {
            $this->compilePostProcessorsForEvent($eventName);
        }

        return $this->postProcessors[$eventName];
    }

    /**
     * Compiles listener configs into actionable listeners.
     * @param string $eventName
     * @return void
     * @throws RuntimeException If listener's callback method does not exist in
     *                          listener service.
     */
    protected function compileListenersForEvent($eventName)
    {
        $this->listeners[$eventName] = [];

        if (!isset($this->listenerConfigs[$eventName])) {
            return;
        }

        foreach ($this->listenerConfigs[$eventName] as $listenerConfig) {
            $service = $this->getService($listenerConfig->serviceId);
            $method = $listenerConfig->method;

            if (!method_exists($service, $method)) {
                throw new \RuntimeException("Listener '" . get_class($service) . "' does not have method '{$method}'");
            }

            $callback = [$service, $method];
            $listener =
                new EventQueueListener(
                    $listenerConfig->id,
                    $callback,
                    $listenerConfig->requires
                );
            $this->listeners[$eventName][] = $listener;
        }
    }

    /**
     * Compiles pre-processor service IDs into services.
     * @param string $eventName
     * @return void
     * @throws RuntimeException If pre-processor service does not implement
     *                          EventQueuePreProcessor.
     */
    protected function compilePreProcessorsForEvent($eventName)
    {
        $this->preProcessors[$eventName] = [];

        if (!isset($this->preProcessorServiceIds[$eventName])) {
            return;
        }

        foreach ($this->preProcessorServiceIds[$eventName] as $serviceId) {
            $service = $this->getService($serviceId);

            if (!($service instanceof EventQueuePreProcessorInterface)) {
                throw new \RuntimeException("Pre-processor '" . get_class($service) . "' does not implement EventQueuePreProcessorInterface");
            }

            $this->preProcessors[$eventName][] = $service;
        }
    }

    /**
     * Compiles post-processor service IDs into services.
     * @param string $eventName
     * @return void
     * @throws RuntimeException If post-processor service does not implement
     *                          EventQueuePostProcessorInterface.
     */
    protected function compilePostProcessorsForEvent($eventName)
    {
        $this->postProcessors[$eventName] = [];

        if (!isset($this->postProcessorServiceIds[$eventName])) {
            return;
        }

        foreach ($this->postProcessorServiceIds[$eventName] as $serviceId) {
            $service = $this->getService($serviceId);

            if (!($service instanceof EventQueuePostProcessorInterface)) {
                throw new \RuntimeException("Post-processor '" . get_class($service) . "' does not implement EventQueuePostProcessorInterface");
            }

            $this->postProcessors[$eventName][] = $service;
        }
    }

    /**
     * Returns service object from the service container.
     * @param string $serviceId
     * @return object
     * @throws RuntimeException If service doesn't exist.
     */
    protected function getService($serviceId)
    {
        if (!$this->serviceContainer->has($serviceId)) {
            throw new \RuntimeException("'{$serviceId}' is not a defined service");
        }

        return $this->serviceContainer->get($serviceId);
    }

}
