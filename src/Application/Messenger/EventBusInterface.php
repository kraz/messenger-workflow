<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Application\Messenger;

interface EventBusInterface
{
    public function publish(object $event): void;
}
