<?php

declare(strict_types=1);

namespace Contracts\Demo\Command;

use Kraz\MessengerWorkflow\Application\CommandInterface;

final class DoSomethingCommand implements CommandInterface
{
    public function __construct(
        public string $what = 'it',
    ) {
    }
}
