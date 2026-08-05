<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Fixture\Retry;

use Kraz\MessengerWorkflow\Infrastructure\Messenger\Retry\RetryDeciderInterface;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\Retry\ThrowableChain;
use Symfony\Component\Messenger\Envelope;

/**
 * Application-registered retry decider (autoconfigured via RetryDeciderInterface):
 * forces a retry when a ForceRetryOnceException appears in the causal chain.
 */
final class ForcedRetryDecider implements RetryDeciderInterface
{
    public function decide(Envelope $envelope, \Throwable $throwable): ?bool
    {
        foreach (ThrowableChain::unwrap($throwable) as $exception) {
            if ($exception instanceof ForceRetryOnceException) {
                return true;
            }
        }

        return null;
    }
}
