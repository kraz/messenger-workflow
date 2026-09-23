<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Fixture\Message\Routing;

use Kraz\MessengerWorkflow\Application\Attribute\MessageRoute;
use Kraz\MessengerWorkflow\Application\CommandInterface;

#[MessageRoute('slow')]
class AttributeRoutedCommand implements CommandInterface
{
}
