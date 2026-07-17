<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Functional;

use Kraz\MessengerWorkflow\Messenger\Transport\InMemoryResultStorage;
use Kraz\MessengerWorkflow\Tests\Fixture\ResultStorageConsumer;
use Kraz\MessengerWorkflow\Tests\TestKernel\ResultStorageAutowireKernel;
use Kraz\MessengerWorkflow\Tests\Support\WorkflowKernelTestCase;

final class ResultStorageAutowiringTest extends WorkflowKernelTestCase
{
    protected static function getKernelClass(): string
    {
        return ResultStorageAutowireKernel::class;
    }

    /**
     * Regression test: the bundle used to register
     * ResultStorageInterface as an instantiable service instead of aliasing
     * messenger_workflow.result_storage, so application services autowiring the
     * interface did not receive the configured storage.
     */
    public function testResultStorageInterfaceIsAutowirableInApplicationServices(): void
    {
        $consumer = self::getContainer()->get(ResultStorageConsumer::class);

        self::assertInstanceOf(InMemoryResultStorage::class, $consumer->storage);

        // and it must be the same instance the middleware/listeners write to
        $consumer->storage->write('autowire-check', 'ok');
        self::assertSame('ok', self::getContainer()->get('messenger_workflow.result_storage')->await('autowire-check', 1));
    }
}
