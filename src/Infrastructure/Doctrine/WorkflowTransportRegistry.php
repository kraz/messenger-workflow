<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Infrastructure\Doctrine;

use Kraz\MessengerWorkflow\Infrastructure\Doctrine\Inbox\InboxTransport;
use Kraz\MessengerWorkflow\Infrastructure\Doctrine\Outbox\OutboxTransport;

/**
 * Tracks the workflow Doctrine transports created in this process by transport name.
 * Used by the command bus deadlock guard and by the transaction middleware to resolve
 * the receiving transport's database and transactional mode.
 */
final class WorkflowTransportRegistry
{
    /**
     * @var array<string, OutboxTransport>
     */
    private array $outboxTransports = [];

    /**
     * @var array<string, InboxTransport>
     */
    private array $inboxTransports = [];

    /**
     * @var array<string, bool>
     */
    private array $transactionalInboxes = [];

    /**
     * @var array<string, string|null>
     */
    private array $inboxConnectionNames = [];

    public function addOutboxTransport(string $transportName, OutboxTransport $transport): void
    {
        $this->outboxTransports[$transportName] = $transport;
    }

    public function getOutboxTransport(string $transportName): ?OutboxTransport
    {
        return $this->outboxTransports[$transportName] ?? null;
    }

    /**
     * @return array<string, OutboxTransport>
     */
    public function getOutboxTransports(): array
    {
        return $this->outboxTransports;
    }

    public function addInboxTransport(string $transportName, InboxTransport $transport, bool $transactionalHandler, ?string $connectionName = null): void
    {
        $this->inboxTransports[$transportName] = $transport;
        $this->transactionalInboxes[$transportName] = $transactionalHandler;
        $this->inboxConnectionNames[$transportName] = $connectionName;
    }

    /**
     * The Doctrine DBAL connection name the inbox transport runs on, as recorded by
     * the transport factory — null when the transport was registered without it.
     */
    public function getInboxConnectionName(string $transportName): ?string
    {
        return $this->inboxConnectionNames[$transportName] ?? null;
    }

    public function getInboxTransport(string $transportName): ?InboxTransport
    {
        return $this->inboxTransports[$transportName] ?? null;
    }

    public function isInboxTransactional(string $transportName): bool
    {
        return $this->transactionalInboxes[$transportName] ?? false;
    }

    /**
     * @return array<string, InboxTransport>
     */
    public function getInboxTransports(): array
    {
        return $this->inboxTransports;
    }
}
