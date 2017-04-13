<?php
namespace Celltrak\EventQueueBundle\Component;

use CTLib\Component\Security\WebService\WebServiceRequestAuthenticatorInterface;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\HttpFoundation\Response;
use CTLib\Component\Monolog\Logger;


/**
 * Verifies that event queue REST API requests are authorized.
 *
 * @author Mike Turoff
 */
class EventQueueRestApiAuthenticator
    implements WebServiceRequestAuthenticatorInterface
{

    /**
     * Number of seconds before a request is considered invalid.
     */
    const REQUEST_EXPIRY_SECONDS = 5;


    /**
     * Secret key used to sign API requests.
     * @var string
     */
    protected $authenticationKey;

    /**
     * Hashing algorithm used for request signatures.
     * @var string
     */
    protected $authenticationAlgorithm;

    /**
     * @var Logger
     */
    protected $logger;


    /**
     * @param string $authenticationKey Secret key used to sign requests.
     * @param string $authenticationAlgorithm Hashing algorithm used for request signatures.
     * @param Logger $logger
     */
    public function __construct(
        $authenticationKey,
        $authenticationAlgorithm,
        Logger $logger
    ) {
        $this->authenticationKey         = $authenticationKey;
        $this->authenticationAlgorithm   = $authenticationAlgorithm;
        $this->logger                    = $logger;
    }

    /**
     * {@inheritDoc}
     */
    public function isHandledRequest(Request $request)
    {
        $path = $request->getPathInfo();
        return strpos($path, '/eventQueue') === 0;
    }

    /**
     * {@inheritDoc}
     */
    public function isAuthenticatedRequest(Request $request)
    {
        if (is_null($this->authenticationKey)) {
            // Configuration expicitly permits any request.
            $this->logger->debug("EventQueueRestApiAuthenticator: No API authentication key in use so allowing all requests");
            return true;
        }

        $receivedAuthToken = $request->headers->get('authorization');

        if (empty($receivedAuthToken)) {
            $this->logger->debug("EventQueueRestApiAuthenticator: 'Authorization' header not provided");
            return false;
        }

        $expectedAuthToken = $this->createAuthToken($request);

        if ($receivedAuthToken !== $expectedAuthToken) {
            $this->logger->debug("EventQueueRestApiAuthenticator: receivedAuthToken '{$receivedAuthToken}' does not match expectedAuthToken '{$expectedAuthToken}'");
            return false;
        }

        $requestAge = $this->getRequestAge($request);

        if ($requestAge > self::REQUEST_EXPIRY_SECONDS) {
            $this->logger->debug("EventQueueRestApiAuthenticator: request expired ({$requestAge} seconds old)");
            return false;
        }

        return true;
    }

    /**
     * {@inheritDoct}
     */
    public function getAuthenticationFailureResponse(Request $request)
    {
        return new Response('', 401);
    }

    /**
     * Creates the authentication token based on the request.
     * @param Request $request
     * @return string
     */
    protected function createAuthToken(Request $request)
    {
        $uri = $request->getRequestUri();
        $authToken = hash_hmac(
            $this->authenticationAlgorithm,
            $uri,
            $this->authenticationKey
        );

        $this->logger->debug("EventQueueRestApiAuthenticator: created authToken '{$authToken}' for uri '{$uri}'");

        return $authToken;
    }

    /**
     * Returns age of request in seconds.
     * @param Request $request
     * @return integer
     */
    protected function getRequestAge(Request $request)
    {
        // Convert request time (ts) from milliseconds to seconds.
        $requestTime = $request->query->get('ts') / 100;
        return time() - $requestTime;
    }

}
