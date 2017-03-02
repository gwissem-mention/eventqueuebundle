<?php
namespace Celltrak\EventQueueBundle\Component;

use CTLib\Component\Security\WebService\WebServiceRequestAuthenticatorInterface;
use Symfony\Component\HttpFoundation\Request;
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

        // TODO change header to 'authentication'
        $receivedAuthToken = $request->headers->get('authorization');

        if (!$receivedAuthToken) {
            $this->logger->debug("EventQueueRestApiAuthenticator: 'Authorization' header not provided");
            return false;
        }

        $receivedAuthToken = base64_decode($receivedAuthToken);

        $uri = $request->getUri();
        $expectedAuthToken =
            hash_hmac($this->authenticationAlgorithm, $uri, $this->authenticationKey);

        return $receivedAuthToken === $expectedAuthToken;
    }

}
