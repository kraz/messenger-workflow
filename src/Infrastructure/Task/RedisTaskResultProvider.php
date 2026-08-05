<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Infrastructure\Task;

use Kraz\MessengerWorkflow\Application\Task\Exception\TaskNotFoundException;
use Kraz\MessengerWorkflow\Application\Task\ResultStoragePayload;
use Kraz\MessengerWorkflow\Application\Task\TaskResultProviderInterface;
use Symfony\Component\Messenger\Exception\TransportException;
use Symfony\Component\VarExporter\DeepCloner;

/**
 * Non-blocking read of a stored task result (single `GET rs:[<ns>:]<id>`, no TTL
 * mutation) — the read counterpart of CommandBusInterface::await() (B8/D2).
 */
final readonly class RedisTaskResultProvider implements TaskResultProviderInterface
{
    public function __construct(
        private \Redis $redis,
        private ?string $resultNamespace = null,
    ) {
    }

    public function getResult(string $taskId): mixed
    {
        try {
            $payload = $this->redis->get($this->storageKey($taskId));
        } catch (\RedisException $exception) {
            throw new TransportException($exception->getMessage(), $exception->getCode(), $exception);
        }

        if (!\is_string($payload) || '' === $payload) {
            throw new TaskNotFoundException(\sprintf('No result exists for task "%s" (unknown, still pending, or expired).', $taskId));
        }

        $data = json_decode($payload, true, 512, \JSON_THROW_ON_ERROR);
        if (!\is_array($data)) {
            throw new TransportException(\sprintf('Unexpected result payload for task "%s".', $taskId));
        }

        $result = DeepCloner::fromArray($data)->clone();

        if (!$result instanceof ResultStoragePayload) {
            return $result;
        }

        if ($result->isError()) {
            throw $result->createFailure();
        }

        return $result->getValue();
    }

    private function storageKey(string $taskId): string
    {
        return 'rs:'.(null !== $this->resultNamespace ? $this->resultNamespace.':' : '').$taskId;
    }
}
