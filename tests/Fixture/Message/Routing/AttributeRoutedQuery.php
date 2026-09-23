<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Fixture\Message\Routing;

use Kraz\MessengerWorkflow\Application\Attribute\MessageRoute;
use Kraz\MessengerWorkflow\Application\QueryInterface;

#[MessageRoute('reports')]
final class AttributeRoutedQuery implements QueryInterface
{
}
