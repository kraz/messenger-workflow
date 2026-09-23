<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Fixture\Handler;

use Contracts\Demo\Command\PlanSomethingCommand;
use Contracts\Demo\Command\ReplanSomethingCommand;
use Kraz\MessengerWorkflow\Application\Attribute\AsCommandHandler;

/**
 * Handler of the routed Demo commands: records the payloads in handling order (the
 * integration workers run in the test process) and fails on demand.
 */
final class PlanSomethingCommandHandler
{
    /**
     * @var list<string>
     */
    public static array $handled = [];

    public static function reset(): void
    {
        self::$handled = [];
    }

    #[AsCommandHandler]
    public function plan(PlanSomethingCommand $command): string
    {
        return self::handle($command->payload);
    }

    #[AsCommandHandler]
    public function replan(ReplanSomethingCommand $command): string
    {
        return self::handle($command->payload);
    }

    private static function handle(string $payload): string
    {
        self::$handled[] = $payload;

        if (str_starts_with($payload, 'fail')) {
            throw new \RuntimeException('planning failed permanently');
        }

        return 'planned:'.$payload;
    }
}
