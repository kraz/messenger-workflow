<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Fixture\Handler;

use Kraz\MessengerWorkflow\Application\Attribute\AsCommandHandler;
use Kraz\MessengerWorkflow\Tests\Fixture\Message\TestCommand;

/**
 * Invalid fixture: a method-level handler attribute must not declare a "method".
 * Only registered by the dedicated kernel of the failure-path test.
 */
final class InvalidMethodDeclaringHandler
{
    #[AsCommandHandler(method: 'handle')]
    public function handle(TestCommand $command): void
    {
    }
}
