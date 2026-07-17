<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Fixture\Handler;

use Kraz\MessengerWorkflow\Application\Attribute\AsQueryHandler;
use Kraz\MessengerWorkflow\Tests\Fixture\Message\TestQuery;
use Kraz\MessengerWorkflow\Tests\Fixture\MessageRecorder;

#[AsQueryHandler]
final readonly class TestQueryHandler
{
    public function __construct(
        private MessageRecorder $recorder,
    ) {
    }

    public function __invoke(TestQuery $query): string
    {
        $this->recorder->record($query);

        return mb_strtoupper($query->subject);
    }
}
