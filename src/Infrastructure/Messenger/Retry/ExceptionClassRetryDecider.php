<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Infrastructure\Messenger\Retry;

use Symfony\Component\Messenger\Envelope;

/**
 * Applies the user-configured exception class lists
 * (messenger_workflow.messenger.defaults.*_retry.{retryable,non_retryable}_exceptions).
 * Non-retryable classes win over retryable ones. Matching is instanceof-based across
 * the whole causal chain of the failure.
 */
final class ExceptionClassRetryDecider implements RetryDeciderInterface
{
    /**
     * @param list<class-string> $retryableExceptions
     * @param list<class-string> $nonRetryableExceptions
     */
    public function __construct(
        private readonly array $retryableExceptions = [],
        private readonly array $nonRetryableExceptions = [],
    ) {
    }

    public function decide(Envelope $envelope, \Throwable $throwable): ?bool
    {
        $verdict = null;
        foreach (ThrowableChain::unwrap($throwable) as $exception) {
            foreach ($this->nonRetryableExceptions as $class) {
                if ($exception instanceof $class) {
                    return false;
                }
            }
            foreach ($this->retryableExceptions as $class) {
                if ($exception instanceof $class) {
                    $verdict = true;
                }
            }
        }

        return $verdict;
    }
}
