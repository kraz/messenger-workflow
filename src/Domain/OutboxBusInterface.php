<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Domain;

interface OutboxBusInterface
{
    public function publish(DomainEventInterface $event): void;
}
