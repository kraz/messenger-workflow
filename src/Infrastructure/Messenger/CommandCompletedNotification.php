<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Infrastructure\Messenger;

use Kraz\MessengerWorkflow\Application\Task\ResultStoragePayload;
use Symfony\Component\VarExporter\DeepCloner;

/**
 * Internal message written to the notifier outbox when a tracked command completes,
 * relayed by the notifier worker into the result storage.
 *
 * The handler result travels in its DeepCloner array form (the same encoding the
 * result storage itself uses), captured in the handler process by forResult(): the
 * value the notifier worker finally writes is identical to what a direct handler-side
 * write would have produced, regardless of the transport serializer in between.
 */
class CommandCompletedNotification
{
    /**
     * @param array<array-key, mixed> $resultPayload
     */
    public function __construct(
        protected string $commandId,
        protected array $resultPayload,
    ) {
    }

    public static function forResult(string $commandId, mixed $result): self
    {
        // Mirrors ResultStorageInterface::write() semantics: object results are stored
        // as-is, everything else is wrapped in a ResultStoragePayload.
        $data = \is_object($result) ? $result : new ResultStoragePayload($result);

        return new self($commandId, new DeepCloner($data)->toArray());
    }

    public function getCommandId(): string
    {
        return $this->commandId;
    }

    /**
     * @return array<array-key, mixed>
     */
    public function getResultPayload(): array
    {
        return $this->resultPayload;
    }

    /**
     * Restores the result captured by forResult().
     */
    public function restoreResult(): mixed
    {
        return DeepCloner::fromArray($this->resultPayload)->clone();
    }
}
