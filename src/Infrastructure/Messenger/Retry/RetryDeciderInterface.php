<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Infrastructure\Messenger\Retry;

use Symfony\Component\Messenger\Envelope;

/**
 * A voter in the retry decision chain. Deciders are consulted in priority order
 * by the RetryDecidingStrategy; the first non-null verdict wins. Application services
 * implementing this interface are autoconfigured into the chain via the
 * "messenger_workflow.retry_decider" tag.
 */
interface RetryDeciderInterface
{
    /**
     * @return bool|null true = retry, false = fail permanently, null = no opinion
     *                   (the next decider in the chain is asked)
     */
    public function decide(Envelope $envelope, \Throwable $throwable): ?bool;
}
