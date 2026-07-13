<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Messenger\Transport;

use Kraz\MessengerWorkflow\Application\Exception\TaskFailedException;
use Kraz\MessengerWorkflow\Messenger\Transport\Exception\ResultStorageWaitTimeoutException;
use Symfony\Contracts\Service\ResetInterface;

class InMemoryResultStorage implements ResultStorageInterface, ResetInterface
{
    private array $data = [];

    public function write(string $messageId, mixed $data): void
    {
        $this->data[$messageId] = $data;
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
        if (!\array_key_exists($messageId, $this->data)) {
            throw new ResultStorageWaitTimeoutException('Waiting for result timeout');
        }

        $result = $this->data[$messageId];

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

    public function reset(): void
    {
        $this->data = [];
    }
}
