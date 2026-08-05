<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Infrastructure\Task;

use Kraz\MessengerWorkflow\Application\Task\TaskOwnership;
use Kraz\MessengerWorkflow\Application\Task\TaskOwnershipRegistryInterface;
use Psr\Log\LoggerInterface;

/**
 * Redis-backed ownership registry, typically sharing the result storage client.
 *
 * Key: `task-meta:<taskId>`; value: small JSON; the TTL defaults to 4h so ownership
 * always outlives the 3h result TTL (a pending/completed task stays authorizable for
 * as long as its result may exist).
 */
final readonly class RedisTaskOwnershipRegistry implements TaskOwnershipRegistryInterface
{
    private const string KEY_PREFIX = 'task-meta:';

    public function __construct(
        private \Redis $redis,
        private ?LoggerInterface $logger = null,
        private int $ownershipTtl = 4 * 60 * 60,
    ) {
    }

    public function record(string $taskId, string $userIdentifier, ?string $messageType = null): void
    {
        try {
            $payload = json_encode([
                'userIdentifier' => $userIdentifier,
                'messageType' => $messageType,
                'startedAt' => new \DateTimeImmutable()->format(\DateTimeInterface::ATOM),
            ], \JSON_THROW_ON_ERROR);

            $this->redis->setex(self::KEY_PREFIX.$taskId, $this->ownershipTtl, $payload);
        } catch (\Throwable $exception) {
            // Best-effort: never fail the dispatch because ownership could not be recorded.
            $this->logger?->warning('Task ownership record failed: '.$exception->getMessage(), ['taskId' => $taskId]);
        }
    }

    public function find(string $taskId): ?TaskOwnership
    {
        try {
            $raw = $this->redis->get(self::KEY_PREFIX.$taskId);
            if (!\is_string($raw) || '' === $raw) {
                return null;
            }

            $data = json_decode($raw, true, 512, \JSON_THROW_ON_ERROR);
            if (!\is_array($data) || !\is_string($data['userIdentifier'] ?? null) || !\is_string($data['startedAt'] ?? null)) {
                return null;
            }

            return new TaskOwnership(
                $data['userIdentifier'],
                \is_string($data['messageType'] ?? null) ? $data['messageType'] : null,
                new \DateTimeImmutable($data['startedAt']),
            );
        } catch (\Throwable $exception) {
            $this->logger?->warning('Task ownership lookup failed: '.$exception->getMessage(), ['taskId' => $taskId]);

            return null;
        }
    }
}
