<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Fixture\Handler;

use Kraz\MessengerWorkflow\Application\Attribute\AsQueryHandler;
use Kraz\MessengerWorkflow\Tests\Fixture\Message\DuplicatedQuery;
use Kraz\MessengerWorkflow\Tests\Fixture\MessageRecorder;

/**
 * Deliberately registers two handlers for the same query — used to test that the
 * "exactly one handler" rule for commands/queries is enforced at consume time.
 */
final class DuplicatedQueryHandlers
{
    public function __construct(private readonly MessageRecorder $recorder)
    {
    }

    #[AsQueryHandler]
    public function first(DuplicatedQuery $query): string
    {
        $this->recorder->record(self::class.'::first', $query);

        return 'first';
    }

    #[AsQueryHandler]
    public function second(DuplicatedQuery $query): string
    {
        $this->recorder->record(self::class.'::second', $query);

        return 'second';
    }
}
