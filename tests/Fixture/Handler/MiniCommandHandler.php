<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Fixture\Handler;

use Contracts\Mini\Command\MiniCommand;
use Doctrine\DBAL\Connection;
use Kraz\MessengerWorkflow\Application\Attribute\AsCommandHandler;

/**
 * No-inbox flow: consumed straight from the broker queue; the
 * queue is orm-mapped, so the workflow transaction middleware must wrap the handler
 * in a transaction on the mapped connection.
 */
#[AsCommandHandler]
final class MiniCommandHandler
{
    /**
     * @var list<bool>
     */
    public static array $inTransaction = [];

    public static function reset(): void
    {
        self::$inTransaction = [];
    }

    public function __construct(private readonly Connection $postgres)
    {
    }

    public function __invoke(MiniCommand $command): string
    {
        self::$inTransaction[] = $this->postgres->isTransactionActive();

        return 'mini:'.$command->payload;
    }
}
