<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Application\Attribute;

use Symfony\Component\Messenger\Attribute\AsMessageHandler;

#[\Attribute(\Attribute::TARGET_CLASS | \Attribute::TARGET_METHOD | \Attribute::IS_REPEATABLE)]
class AsCommandHandler extends AsMessageHandler
{
}
