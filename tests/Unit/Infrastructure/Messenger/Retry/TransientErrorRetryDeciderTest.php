<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Unit\Infrastructure\Messenger\Retry;

use Contracts\Demo\Command\DoSomethingCommand;
use Doctrine\DBAL\Driver\PDO\Exception as PdoDriverException;
use Doctrine\DBAL\Exception\ConnectionLost;
use Doctrine\DBAL\Exception\DeadlockException;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\Retry\TransientErrorRetryDecider;
use PhpAmqpLib\Exception\AMQPConnectionClosedException;
use PhpAmqpLib\Exception\AMQPIOException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\HandlerFailedException;
use Symfony\Component\Messenger\Exception\TransportException;

final class TransientErrorRetryDeciderTest extends TestCase
{
    private TransientErrorRetryDecider $decider;

    protected function setUp(): void
    {
        $this->decider = new TransientErrorRetryDecider();
    }

    private function envelope(): Envelope
    {
        return new Envelope(new DoSomethingCommand('p'));
    }

    /**
     * @return iterable<string, array{\Throwable}>
     */
    public static function transientExceptions(): iterable
    {
        yield 'dbal connection lost' => [new ConnectionLost(self::pdoDriverException(), null)];
        yield 'dbal deadlock' => [new DeadlockException(self::pdoDriverException(), null)];
        yield 'amqp connection closed' => [new AMQPConnectionClosedException('gone')];
        yield 'amqp io error' => [new AMQPIOException('io')];
        yield 'messenger transport error' => [new TransportException('broker unavailable')];
    }

    private static function pdoDriverException(): PdoDriverException
    {
        return PdoDriverException::new(new \PDOException('server closed the connection'));
    }

    #[DataProvider('transientExceptions')]
    public function testTransientInfrastructureErrorsForceARetry(\Throwable $throwable): void
    {
        self::assertTrue($this->decider->decide($this->envelope(), $throwable));
    }

    public function testTransientErrorsNestedInHandlerFailuresAreDetected(): void
    {
        $wrapped = new HandlerFailedException(
            $this->envelope(),
            [new \RuntimeException('handler wrapper', 0, new ConnectionLost(self::pdoDriverException(), null))],
        );

        self::assertTrue($this->decider->decide($this->envelope(), $wrapped));
    }

    public function testOrdinaryFailuresYieldNoOpinion(): void
    {
        self::assertNull($this->decider->decide($this->envelope(), new \RuntimeException('domain failure')));
        self::assertNull($this->decider->decide($this->envelope(), new \InvalidArgumentException('bad input')));
    }
}
