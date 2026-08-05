<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Fixture\Handler;

use Contracts\Demo\Query\AmbiguousQuery;
use Contracts\Demo\Query\GetSomethingQuery;
use Kraz\MessengerWorkflow\Application\Attribute\AsQueryHandler;
use PhpAmqpLib\Exception\AMQPConnectionClosedException;

/**
 * E2E query handler with payload-selected behavior (attempts counted per payload in
 * static state — the integration workers run in the test process). Also declares two
 * handlers for AmbiguousQuery to exercise the exactly-one enforcement.
 */
final class GetSomethingQueryHandler
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

    /**
     * @return array<string, string|int>
     */
    #[AsQueryHandler]
    public function __invoke(GetSomethingQuery $query): array
    {
        $attempt = self::$attempts[$query->payload] = self::attempts($query->payload) + 1;

        return match ($query->payload) {
            'fail' => throw new \RuntimeException('query failed permanently', 23),
            'transient-once' => $attempt > 1
                ? ['answer' => 'transient-recovered', 'attempt' => $attempt]
                : throw new AMQPConnectionClosedException('broker connection lost'),
            default => ['answer' => 'value:'.$query->payload, 'attempt' => $attempt],
        };
    }

    #[AsQueryHandler]
    public function ambiguousFirst(AmbiguousQuery $query): string
    {
        return 'first';
    }

    #[AsQueryHandler]
    public function ambiguousSecond(AmbiguousQuery $query): string
    {
        return 'second';
    }
}
