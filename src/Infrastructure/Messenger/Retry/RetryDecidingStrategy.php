<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Infrastructure\Messenger\Retry;

use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Retry\RetryStrategyInterface;
use Symfony\Component\Messenger\Stamp\RedeliveryStamp;

/**
 * Workflow retry strategy: a chain of RetryDeciderInterface voters decides
 * WHETHER to retry, the wrapped delay strategy (MultiplierRetryStrategy — multiplier,
 * max_delay, jitter) decides HOW LONG to wait and caps the attempt count. A total
 * retry-time budget bounds how long a message may keep failing across all attempts
 * (measured from the first redelivery), so a poison message cannot occupy a queue
 * forever.
 *
 * Symfony's retry listener already implements the native deciders — a
 * RecoverableMessageHandlingException always retries and an
 * UnrecoverableMessageHandlingException never does — before this strategy is
 * consulted.
 */
final class RetryDecidingStrategy implements RetryStrategyInterface
{
    /**
     * The decider chain may nest one level of iterables so DI configuration can splice
     * a tagged-service iterator between the built-in deciders (configured exception
     * lists first, application deciders, the transient decider last).
     *
     * @param iterable<RetryDeciderInterface|iterable<RetryDeciderInterface>> $deciders        Consulted in order; first non-null verdict wins
     * @param RetryStrategyInterface                                          $delayStrategy   Enforces max attempts and computes waiting times
     * @param int                                                             $maxTotalDelayMs Total retry-time budget in milliseconds (0 = unbounded)
     * @param bool                                                            $retryByDefault  Verdict when no decider has an opinion (false for commands, true for events)
     */
    public function __construct(
        private readonly iterable $deciders,
        private readonly RetryStrategyInterface $delayStrategy,
        private readonly int $maxTotalDelayMs = 0,
        private readonly bool $retryByDefault = false,
    ) {
    }

    public function isRetryable(Envelope $message, ?\Throwable $throwable = null): bool
    {
        $verdict = null;
        if (null !== $throwable) {
            foreach ($this->allDeciders() as $decider) {
                $verdict = $decider->decide($message, $throwable);
                if (null !== $verdict) {
                    break;
                }
            }
        }

        if (!($verdict ?? $this->retryByDefault)) {
            return false;
        }

        if (!$this->isWithinTotalDelayBudget($message)) {
            return false;
        }

        return $this->delayStrategy->isRetryable($message, $throwable);
    }

    public function getWaitingTime(Envelope $message, ?\Throwable $throwable = null): int
    {
        return $this->delayStrategy->getWaitingTime($message, $throwable);
    }

    /**
     * @return iterable<RetryDeciderInterface>
     */
    private function allDeciders(): iterable
    {
        foreach ($this->deciders as $decider) {
            if ($decider instanceof RetryDeciderInterface) {
                yield $decider;
                continue;
            }
            yield from $decider;
        }
    }

    private function isWithinTotalDelayBudget(Envelope $envelope): bool
    {
        if (0 >= $this->maxTotalDelayMs) {
            return true;
        }

        // The oldest RedeliveryStamp marks the first failed attempt.
        $redeliveryStamps = $envelope->all(RedeliveryStamp::class);
        $firstRedelivery = $redeliveryStamps[0] ?? null;
        if (!$firstRedelivery instanceof RedeliveryStamp) {
            return true;
        }

        $elapsedMs = (microtime(true) - (float) $firstRedelivery->getRedeliveredAt()->format('U.u')) * 1000;

        return $elapsedMs < $this->maxTotalDelayMs;
    }
}
