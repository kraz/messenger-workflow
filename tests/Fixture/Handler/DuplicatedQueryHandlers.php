<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Fixture\Handler;

use Kraz\MessengerWorkflow\Application\Attribute\AsQueryHandler;
use Kraz\MessengerWorkflow\Tests\Fixture\Message\DuplicatedQuery;

final readonly class DuplicatedQueryHandlers
{
    #[AsQueryHandler]
    public function first(DuplicatedQuery $query): string
    {
        return 'first';
    }

    #[AsQueryHandler]
    public function second(DuplicatedQuery $query): string
    {
        return 'second';
    }
}
