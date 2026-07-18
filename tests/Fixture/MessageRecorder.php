<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Fixture;

use Symfony\Contracts\Service\ResetInterface;

final class MessageRecorder implements ResetInterface
{
    /** @var list<object> */
    public array $messages = [];

    public function record(object $message): void
    {
        $this->messages[] = $message;
    }

    /**
     * @return list<object>
     */
    public function ofType(string $class): array
    {
        return array_values(array_filter($this->messages, static fn (object $m) => $m instanceof $class));
    }

    public function reset(): void
    {
        $this->messages = [];
    }
}
