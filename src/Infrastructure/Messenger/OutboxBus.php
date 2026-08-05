<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Infrastructure\Messenger;

use Kraz\MessengerWorkflow\Domain\DomainEventInterface;
use Kraz\MessengerWorkflow\Domain\OutboxBusInterface;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\MessageBusInterface;
use Symfony\Component\Messenger\Stamp\TransportNamesStamp;

/**
 * Generic per-context outbox bus: publishes a domain event through the configured
 * outbox transport of the bounded context (overriding the broker routing with a
 * TransportNamesStamp) — the outbox relay then forwards it to the broker.
 *
 * Instances are registered from `messenger_workflow.messenger.outbox_buses`
 * (`<context>: <outbox transport>`), replacing the hand-written per-context
 * `<Ctx>OutboxBus` classes of the original package (which remain possible).
 */
class OutboxBus implements OutboxBusInterface
{
    public function __construct(
        private readonly MessageBusInterface $messageBus,
        private readonly string $transportName,
    ) {
    }

    public function publish(DomainEventInterface $event): void
    {
        $this->messageBus->dispatch(
            Envelope::wrap($event)->with(new TransportNamesStamp([$this->transportName])),
        );
    }
}
