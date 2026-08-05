<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Fixture;

/**
 * Records which fixture handlers were invoked with which messages.
 */
final class MessageRecorder
{
    /**
     * @var list<array{handler: string, message: object}>
     */
    private array $records = [];

    public function record(string $handler, object $message): void
    {
        $this->records[] = ['handler' => $handler, 'message' => $message];
    }

    /**
     * @return list<string>
     */
    public function handlersFor(object $message): array
    {
        return array_values(array_map(
            static fn (array $record): string => $record['handler'],
            array_filter($this->records, static fn (array $record): bool => $record['message'] === $message),
        ));
    }

    /**
     * All recorded handler invocations, in order (messages consumed from a transport
     * are deserialized clones, so identity-based lookup does not apply to them).
     *
     * @return list<string>
     */
    public function handlerNames(): array
    {
        return array_map(
            static fn (array $record): string => $record['handler'],
            $this->records,
        );
    }

    public function reset(): void
    {
        $this->records = [];
    }
}
