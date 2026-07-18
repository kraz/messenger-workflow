<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Fixture;

use Kraz\MessengerWorkflow\Messenger\Transport\ResultStorageInterface;

/**
 * Registered only by ResultStorageAutowireKernel — application-style service that
 * autowires the ResultStorageInterface.
 */
final readonly class ResultStorageConsumer
{
    public function __construct(
        public ResultStorageInterface $storage,
    ) {
    }
}
