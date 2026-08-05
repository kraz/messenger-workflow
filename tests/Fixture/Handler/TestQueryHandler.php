<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Fixture\Handler;

use Kraz\MessengerWorkflow\Application\Attribute\AsQueryHandler;
use Kraz\MessengerWorkflow\Tests\Fixture\Message\TestQuery;
use Kraz\MessengerWorkflow\Tests\Fixture\MessageRecorder;

#[AsQueryHandler]
final class TestQueryHandler
{
    public function __construct(private readonly MessageRecorder $recorder)
    {
    }

    public function __invoke(TestQuery $query): string
    {
        $this->recorder->record(self::class, $query);

        return 'query-result:'.$query->payload;
    }
}
