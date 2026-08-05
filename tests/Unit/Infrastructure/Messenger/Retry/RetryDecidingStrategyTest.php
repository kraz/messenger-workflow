<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Unit\Infrastructure\Messenger\Retry;

use Contracts\Demo\Command\DoSomethingCommand;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\Retry\RetryDeciderInterface;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\Retry\RetryDecidingStrategy;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Retry\MultiplierRetryStrategy;
use Symfony\Component\Messenger\Stamp\RedeliveryStamp;

final class RetryDecidingStrategyTest extends TestCase
{
    private function decider(?bool $verdict): RetryDeciderInterface
    {
        return new class($verdict) implements RetryDeciderInterface {
            public function __construct(private readonly ?bool $verdict)
            {
            }

            public function decide(Envelope $envelope, \Throwable $throwable): ?bool
            {
                return $this->verdict;
            }
        };
    }

    /**
     * @param list<RetryDeciderInterface> $deciders
     */
    private function strategy(array $deciders, int $maxRetries = 3, int $maxTotalDelayMs = 0, bool $retryByDefault = false): RetryDecidingStrategy
    {
        return new RetryDecidingStrategy(
            $deciders,
            new MultiplierRetryStrategy($maxRetries, 1000, 1, 0, 0.0),
            $maxTotalDelayMs,
            $retryByDefault,
        );
    }

    private function envelope(int $retryCount = 0, ?\DateTimeImmutable $firstRedeliveredAt = null): Envelope
    {
        $stamps = [];
        if (null !== $firstRedeliveredAt) {
            $stamps[] = new RedeliveryStamp(1, $firstRedeliveredAt);
        }
        if ($retryCount > 0) {
            $stamps[] = new RedeliveryStamp($retryCount);
        }

        return new Envelope(new DoSomethingCommand('p'), $stamps);
    }

    public function testNoOpinionAndNoDefaultMeansNoRetry(): void
    {
        self::assertFalse($this->strategy([$this->decider(null)])->isRetryable($this->envelope(), new \RuntimeException()));
    }

    public function testNoDecidersAndRetryByDefaultRetriesWithinTheAttemptCap(): void
    {
        $strategy = $this->strategy([], maxRetries: 2, retryByDefault: true);

        self::assertTrue($strategy->isRetryable($this->envelope(), new \RuntimeException()));
        self::assertTrue($strategy->isRetryable($this->envelope(1), new \RuntimeException()));
        self::assertFalse($strategy->isRetryable($this->envelope(2), new \RuntimeException()), 'The delay strategy caps the attempts');
    }

    public function testAPositiveVerdictDelegatesTheAttemptCapToTheDelayStrategy(): void
    {
        $strategy = $this->strategy([$this->decider(true)], maxRetries: 3);

        self::assertTrue($strategy->isRetryable($this->envelope(2), new \RuntimeException()));
        self::assertFalse($strategy->isRetryable($this->envelope(3), new \RuntimeException()));
    }

    public function testANegativeVerdictWins(): void
    {
        self::assertFalse($this->strategy([$this->decider(false), $this->decider(true)])->isRetryable($this->envelope(), new \RuntimeException()));
    }

    public function testTheFirstNonNullVerdictStopsTheChain(): void
    {
        $strategy = $this->strategy([$this->decider(null), $this->decider(true), $this->decider(false)]);

        self::assertTrue($strategy->isRetryable($this->envelope(), new \RuntimeException()));
    }

    public function testTheTotalDelayBudgetBoundsRetriesAcrossAttempts(): void
    {
        $strategy = $this->strategy([$this->decider(true)], maxRetries: 100, maxTotalDelayMs: 30_000);

        self::assertTrue($strategy->isRetryable($this->envelope(1, new \DateTimeImmutable('-5 seconds')), new \RuntimeException()));
        self::assertFalse($strategy->isRetryable($this->envelope(1, new \DateTimeImmutable('-31 seconds')), new \RuntimeException()), 'First failure longer ago than the budget → no more retries');
    }

    public function testWithoutAThrowableTheDefaultVerdictApplies(): void
    {
        self::assertFalse($this->strategy([$this->decider(true)])->isRetryable($this->envelope()));
        self::assertTrue($this->strategy([], retryByDefault: true)->isRetryable($this->envelope()));
    }

    public function testWaitingTimeIsDelegatedToTheDelayStrategy(): void
    {
        self::assertSame(1000, $this->strategy([$this->decider(true)])->getWaitingTime($this->envelope(), new \RuntimeException()));
    }
}
