<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Infrastructure\Task;

use Kraz\MessengerWorkflow\Application\Task\Exception\TaskNotFoundException;
use Kraz\MessengerWorkflow\Application\Task\ResultStoragePayload;
use Kraz\MessengerWorkflow\Application\Task\TaskError;
use Kraz\MessengerWorkflow\Application\Task\TaskOwnershipRegistryInterface;
use Kraz\MessengerWorkflow\Application\Task\TaskStatus;
use Kraz\MessengerWorkflow\Application\Task\TaskStatusProviderInterface;
use Kraz\MessengerWorkflow\Application\Task\TaskStatusResponse;
use Psr\Log\LoggerInterface;
use Symfony\Component\VarExporter\DeepCloner;

/**
 * Non-destructive peek over the result storage Redis (single `GET rs:[<ns>:]<id>`),
 * never await() — polling must not block nor shorten the result TTL.
 *
 * | Redis state                              | Status    |
 * |------------------------------------------|-----------|
 * | result exists, payload carries an error  | failed    |
 * | result exists, no error                  | completed |
 * | result exists but cannot be decoded      | unknown   |
 * | no result, ownership record exists       | pending   |
 * | neither                                  | not found |
 */
final readonly class RedisTaskStatusProvider implements TaskStatusProviderInterface
{
    public function __construct(
        private \Redis $redis,
        private TaskOwnershipRegistryInterface $ownership,
        private ?LoggerInterface $logger = null,
        private bool $debug = false,
        private ?string $resultNamespace = null,
    ) {
    }

    public function getStatus(string $taskId): TaskStatusResponse
    {
        $payload = $this->redis->get($this->storageKey($taskId));
        $owner = $this->ownership->find($taskId);

        if (!\is_string($payload) || '' === $payload) {
            if (null === $owner) {
                throw new TaskNotFoundException('Task not found!');
            }

            return new TaskStatusResponse($taskId, TaskStatus::Pending, null, $owner->startedAt);
        }

        [$status, $error] = $this->decode($taskId, $payload);

        return new TaskStatusResponse($taskId, $status, $error, $owner?->startedAt);
    }

    /**
     * @return array{0: TaskStatus, 1: ?TaskError}
     */
    private function decode(string $taskId, string $payload): array
    {
        try {
            $data = json_decode($payload, true, 512, \JSON_THROW_ON_ERROR);
            $result = \is_array($data) ? DeepCloner::fromArray($data)->clone() : $data;
        } catch (\Throwable $exception) {
            // A result we cannot decode proves the task ended, but not how — report
            // "unknown" rather than claiming success; still never surface a 500.
            $this->logger?->warning('Task result decode failed: '.$exception->getMessage(), ['taskId' => $taskId]);

            return [TaskStatus::Unknown, null];
        }

        if ($result instanceof ResultStoragePayload && $result->isError()) {
            return [TaskStatus::Failed, $this->maskError($result->getError() ?? [])];
        }

        return [TaskStatus::Completed, null];
    }

    /**
     * @param array<array-key, mixed> $error
     */
    private function maskError(array $error): TaskError
    {
        $message = \is_string($error['message'] ?? null) ? $error['message'] : 'Task failed';
        $code = $error['code'] ?? null;
        $code = (\is_int($code) || \is_string($code)) ? $code : null;
        $class = ($this->debug && \is_string($error['class'] ?? null)) ? $error['class'] : null;

        return new TaskError($message, $code, $class);
    }

    private function storageKey(string $taskId): string
    {
        return 'rs:'.(null !== $this->resultNamespace ? $this->resultNamespace.':' : '').$taskId;
    }
}
