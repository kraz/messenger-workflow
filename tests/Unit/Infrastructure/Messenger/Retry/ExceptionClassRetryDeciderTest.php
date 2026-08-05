<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Unit\Infrastructure\Messenger\Retry;

use Contracts\Demo\Command\DoSomethingCommand;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\Retry\ExceptionClassRetryDecider;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\HandlerFailedException;

final class ExceptionClassRetryDeciderTest extends TestCase
{
    private function envelope(): Envelope
    {
        return new Envelope(new DoSomethingCommand('p'));
    }

    public function testUnlistedExceptionsYieldNoOpinion(): void
    {
        $decider = new ExceptionClassRetryDecider([\LogicException::class], [\DomainException::class]);

        self::assertNull($decider->decide($this->envelope(), new \RuntimeException()));
    }

    public function testRetryableListForcesARetryInstanceofMatched(): void
    {
        $decider = new ExceptionClassRetryDecider([\RuntimeException::class], []);

        self::assertTrue($decider->decide($this->envelope(), new \RuntimeException()));
        // Subclasses match too.
        self::assertTrue($decider->decide($this->envelope(), new \OutOfBoundsException()));
    }

    public function testNonRetryableListWinsOverRetryable(): void
    {
        $decider = new ExceptionClassRetryDecider([\RuntimeException::class], [\OutOfBoundsException::class]);

        self::assertFalse($decider->decide($this->envelope(), new \OutOfBoundsException()));
        self::assertTrue($decider->decide($this->envelope(), new \RuntimeException()));
    }

    public function testTheCausalChainIsInspected(): void
    {
        $decider = new ExceptionClassRetryDecider([\DomainException::class], []);
        $wrapped = new HandlerFailedException(
            $this->envelope(),
            [new \RuntimeException('wrapper', 0, new \DomainException('cause'))],
        );

        self::assertTrue($decider->decide($this->envelope(), $wrapped));
    }

    public function testEmptyListsHaveNoOpinion(): void
    {
        self::assertNull(new ExceptionClassRetryDecider()->decide($this->envelope(), new \RuntimeException()));
    }
}
