<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Messenger\Transport;

use Kraz\MessengerWorkflow\Application\Exception\TaskFailedException;
use Kraz\MessengerWorkflow\Messenger\Transport\Exception\ResultStorageWaitTimeoutException;
use Symfony\Component\Messenger\Exception\TransportException;
use Symfony\Component\VarExporter\DeepCloner;
use Webmozart\Assert\Assert;

class RedisResultStorage implements ResultStorageInterface
{
    public function __construct(
        protected \Redis $redis,
        protected int $expireInputAfter = 3 * 60 * 60,
        protected int $expireOutputAfter = 60,
        protected ?string $namespace = null,
    ) {
    }

    public function write(string $messageId, mixed $data): void
    {
        if (!\is_object($data)) {
            $data = new ResultStoragePayload($data);
        }
        $data = new DeepCloner($data)->toArray();
        $payload = json_encode($data, \JSON_THROW_ON_ERROR | \JSON_UNESCAPED_SLASHES | \JSON_UNESCAPED_UNICODE);
        $key = $this->getStorageKey($messageId);
        try {
            $this->redis->setex($key, $this->expireInputAfter, $payload);
        } catch (\RedisException $exception) {
            throw new TransportException($exception->getMessage(), $exception->getCode());
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

        do {
            try {
                $payload = $this->redis->get($key);
                if (false === $payload) {
                    usleep(50000);
                } else {
                    $this->redis->expire($key, $this->expireOutputAfter);
                    break;
                }
            } catch (\RedisException $exception) {
                throw new TransportException($exception->getMessage(), $exception->getCode());
            }
        } while (microtime(true) < $deadline);

        if (false === $payload) {
            throw new ResultStorageWaitTimeoutException('Waiting for result timeout');
        }

        Assert::stringNotEmpty($payload, 'The result payload is empty!');

        $data = json_decode($payload, true, 512, \JSON_THROW_ON_ERROR);
        $result = DeepCloner::fromArray($data)->clone();

        if (!$result instanceof ResultStoragePayload) {
            return $result;
        }

        if ($result->isError()) {
            $error = $result->getError();
            $errorMessage = $error['message'] ?? '';
            $errorCode = $error['code'] ?? 0;
            $errorClass = $error['class'] ?? null;
            $errorTrace = $error['trace'] ?? null;
            throw new TaskFailedException($errorMessage, $errorCode, $errorClass, $errorTrace);
        }

        return $result->getValue();
    }

    private function getStorageKey(string $messageId): string
    {
        return 'rs:'.(null !== $this->namespace ? $this->namespace.':' : '').$messageId;
    }
}
