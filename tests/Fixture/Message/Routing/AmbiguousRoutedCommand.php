<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Fixture\Message\Routing;

final class AmbiguousRoutedCommand implements RoutedCommandInterface, OtherRoutedCommandInterface
{
}
