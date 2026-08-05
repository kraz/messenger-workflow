<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Infrastructure\Task;

use Kraz\MessengerWorkflow\Application\Task\Exception\ResultStorageWaitTimeoutException;
use Kraz\MessengerWorkflow\Application\Task\ResultStorageInterface;
use Kraz\MessengerWorkflow\Application\Task\ResultStoragePayload;
use Symfony\Component\Messenger\Exception\TransportException;
use Symfony\Component\VarExporter\DeepCloner;

/**
 * Result storage backed by Redis. Keys: rs:[<namespace>:]<messageId>.
 *
 * await() polls (default every 50 ms) — deliberately no blocking Redis primitives:
 * the client is commonly shared with other services (e.g. the task status provider)
 * and a blocked phpredis connection would stall them; a BLPOP wake key additionally
 * wakes only one of several concurrent awaiters.
 *
 * await() never shortens the result TTL unless $expireAfterAwait is explicitly
 * configured (the old always-EXPIRE behavior broke pollers reading after an await).
 */
class RedisResultStorage implements ResultStorageInterface
{
    public function __construct(
        protected \Redis $redis,
        protected int $expireInputAfter = 3 * 60 * 60,
        protected ?int $expireAfterAwait = null,
        protected ?string $namespace = null,
        protected int $pollIntervalMs = 50,
    ) {
    }

    public function write(string $messageId, mixed $data): void
    {
        if (!\is_object($data)) {
            $data = new ResultStoragePayload($data);
        }

        $payload = json_encode(
            new DeepCloner($data)->toArray(),
            \JSON_THROW_ON_ERROR | \JSON_UNESCAPED_SLASHES | \JSON_UNESCAPED_UNICODE,
        );

        try {
            $this->redis->setex($this->getStorageKey($messageId), $this->expireInputAfter, $payload);
        } catch (\RedisException $exception) {
            throw new TransportException($exception->getMessage(), $exception->getCode(), $exception);
        }
    }

    public function writeError(string $messageId, string $message, int|string|null $code = null, ?string $class = null, ?string $trace = null): void
    {
        $this->write($messageId, new ResultStoragePayload(null, [
            'message' => $message,
            'code' => $code,
            'class' => $class,
            'trace' => $trace,
        ]));
    }

    public function await(string $messageId, int $timeout): mixed
    {
        $key = $this->getStorageKey($messageId);
        $deadline = microtime(true) + $timeout;
        $payload = false;

        do {
            try {
                $payload = $this->redis->get($key);
                if (false !== $payload) {
                    if (null !== $this->expireAfterAwait) {
                        $this->redis->expire($key, $this->expireAfterAwait);
                    }
                    break;
                }
            } catch (\RedisException $exception) {
                throw new TransportException($exception->getMessage(), $exception->getCode(), $exception);
            }

            usleep($this->pollIntervalMs * 1000);
        } while (microtime(true) < $deadline);

        if (!\is_string($payload) || '' === $payload) {
            throw new ResultStorageWaitTimeoutException('Waiting for result timeout');
        }

        $data = json_decode($payload, true, 512, \JSON_THROW_ON_ERROR);
        if (!\is_array($data)) {
            throw new TransportException(\sprintf('Unexpected result payload for message "%s".', $messageId));
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

    private function getStorageKey(string $messageId): string
    {
        return 'rs:'.(null !== $this->namespace ? $this->namespace.':' : '').$messageId;
    }
}
