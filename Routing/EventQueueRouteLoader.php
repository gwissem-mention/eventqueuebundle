<?php
namespace Celltrak\EventQueueBundle\Routing;

use Symfony\Component\Config\Loader\Loader;
use Symfony\Component\Routing\Route;
use Symfony\Component\Routing\RouteCollection;


/**
 * Loads routes used for the Event Queue REST API.
 *
 * @author Mike Turoff
 */
class EventQueueRouteLoader extends Loader
{

    /**
     * URI path to event queue worker endpoint.
     */
    const WORKERS_PATH = '/eventQueue/channels/{channelId}/workers';

    /**
     * Service ID of the event queue worker controller.
     */
    const WORKERS_CONTROLLER_SERVICE = 'event_queue.worker_controller';

    /**
     * URI path to event queue channels endpoing
     */
    const CHANNELS_PATH = '/eventQueue/channels';

    /**
     * Service ID of the event queue channel controller
     */
    const CHANNEL_CONTROLLER_SERVICE = 'event_queue.channel_controller';

    /**
     * URI path to events endpoint.
     */
    const EVENTS_PATH = '/eventQueue/events';

    /**
     * Service ID of the event Events controller.
     */
    const EVENTS_CONTROLLER_SERVICE = 'event_queue.events_controller';

    /**
     * Indicates whether these routes have already been loaded into application.
     * @var boolean
     */
    private $loaded = false;


    /**
     * Loads routes into the application.
     * @param  mixed $resource
     * @param  string $type
     * @return RouteCollection
     */
    public function load($resource, $type = null)
    {
        if (true === $this->loaded) {
            throw new \RuntimeException("Do not add the 'event_queue' route loader twice");
        }

        $this->routes = new RouteCollection();
        $this->loadProvisionWorkerRoute();
        $this->loadKillWorkerRoute();
        $this->loadEventsDispatchRoute();
        $this->loaded = true;

        return $this->routes;
    }

    /**
     * Indicates whether this loader supports the designated $resource and $type.
     * @param  mixed $resource
     * @param  string $type
     * @return boolean
     */
    public function supports($resource, $type = null)
    {
        return 'event_queue' === $type;
    }

    /**
     * Loads the route to provision worker.
     * @return void
     */
    protected function loadProvisionWorkerRoute()
    {
        $path           = self::WORKERS_PATH;
        $controller     = self::WORKERS_CONTROLLER_SERVICE . ':provisionAction';
        $defaults       = ['_controller' => $controller];
        $requirements   = ['_method' => 'POST'];

        $routeName = 'event_queue.provision_worker';
        $route = new Route($path, $defaults, $requirements);
        $this->routes->add($routeName, $route);
    }

    /**
     * Loads the route to kill worker.
     * @return void
     */
    protected function loadKillWorkerRoute()
    {
        $path           = self::WORKERS_PATH . '/{workerId}';
        $controller     = self::WORKERS_CONTROLLER_SERVICE . ':killAction';
        $defaults       = ['_controller' => $controller];
        $requirements   = ['_method' => 'DELETE'];

        $routeName = 'event_queue.kill_worker';
        $route = new Route($path, $defaults, $requirements);
        $this->routes->add($routeName, $route);
    }

    /**
     * Loads the route to events dispatch action.
     * @return void
     */
    protected function loadEventsDispatchRoute()
    {
        $path           = self::EVENTS_PATH;
        $controller     = self::EVENTS_CONTROLLER_SERVICE . ':dispatchAction';
        $defaults       = ['_controller' => $controller];
        $requirements   = ['_method' => 'POST'];

        $routeName = 'event_queue.events_dispatch';
        $route = new Route($path, $defaults, $requirements);
        $this->routes->add($routeName, $route);
    }

}
