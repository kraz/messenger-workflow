<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Functional;

use Kraz\MessengerWorkflow\Application\Task\ResultStorageInterface;
use Kraz\MessengerWorkflow\Infrastructure\Task\InMemoryResultStorage;
use Kraz\MessengerWorkflow\Tests\Support\WorkflowKernelTestCase;

final class ResultStorageAutowiringTest extends WorkflowKernelTestCase
{
    public function testDefaultProviderIsTheInMemoryStorage(): void
    {
        self::bootKernel();

        $storage = self::getContainer()->get(ResultStorageInterface::class);

        self::assertInstanceOf(InMemoryResultStorage::class, $storage);
        self::assertSame($storage, self::getContainer()->get('messenger_workflow.result_storage'));
    }
}
