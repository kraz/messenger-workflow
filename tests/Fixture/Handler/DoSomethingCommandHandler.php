<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Fixture\Handler;

use Contracts\Demo\Command\DoSomethingCommand;
use Kraz\MessengerWorkflow\Application\Attribute\AsCommandHandler;
use Kraz\MessengerWorkflow\Tests\Fixture\Retry\ForceRetryOnceException;
use PhpAmqpLib\Exception\AMQPConnectionClosedException;
use Symfony\Component\Messenger\Exception\RecoverableMessageHandlingException;
use Symfony\Component\Messenger\Exception\UnrecoverableMessageHandlingException;

/**
 * E2E command handler with payload-selected behavior. Attempts are counted per
 * payload in static state — the integration workers run in the test process.
 */
#[AsCommandHandler]
final class DoSomethingCommandHandler
{
    /**
     * @var array<string, int>
     */
    public static array $attempts = [];

    public static function reset(): void
    {
        self::$attempts = [];
    }

    public static function attempts(string $payload): int
    {
        return self::$attempts[$payload] ?? 0;
    }

    public function __invoke(DoSomethingCommand $command): string
    {
        $attempt = self::$attempts[$command->payload] = self::attempts($command->payload) + 1;

        return match ($command->payload) {
            'fail' => throw new \RuntimeException('command failed permanently', 42),
            'unrecoverable' => throw new UnrecoverableMessageHandlingException('command failed unrecoverably'),
            'recoverable-once' => $attempt > 1 ? 'recovered:'.$attempt : throw new RecoverableMessageHandlingException('try again'),
            'transient-once' => $attempt > 1 ? 'transient-recovered:'.$attempt : throw new AMQPConnectionClosedException('broker connection lost'),
            'custom-retry-once' => $attempt > 1 ? 'custom-recovered:'.$attempt : throw new ForceRetryOnceException('please retry'),
            default => 'done:'.$command->payload,
        };
    }
}
