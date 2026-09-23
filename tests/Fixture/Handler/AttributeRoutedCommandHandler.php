<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Fixture\Handler;

use Kraz\MessengerWorkflow\Tests\Fixture\Message\Routing\AttributeRoutedCommand;

/**
 * Registered by hand in compiler-pass unit tests: a handler whose message class
 * carries #[MessageRoute].
 */
final class AttributeRoutedCommandHandler
{
    public function __invoke(AttributeRoutedCommand $command): void
    {
    }
}
