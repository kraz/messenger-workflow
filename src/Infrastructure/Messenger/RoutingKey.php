<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Infrastructure\Messenger;

/**
 * Derives an AMQP routing key from a message FQCN (or plain string).
 *
 * Class names are converted to dotted keys. Messages under the `Contracts\` namespace are
 * public (routed by their path without prefix); everything else is prefixed with
 * `internal.`. For direct exchanges only the bounded-context segment is used; for topic
 * exchanges the full dotted path is kept.
 */
readonly class RoutingKey implements \Stringable
{
    private string $key;

    public function __construct(mixed $data, string $pattern, ?string $prefix = null, ?string $suffix = null)
    {
        $key = match (true) {
            \is_object($data) => $data::class,
            \is_scalar($data) => (string) $data,
            default => throw new \InvalidArgumentException(\sprintf('Cannot derive a routing key from "%s".', get_debug_type($data))),
        };
        if (str_contains($key, '\\')) {
            $key = str_replace('\\', '.', $key);
            $isPublic = str_starts_with($key, 'Contracts.') || str_starts_with($key, '.Contracts.');
            if (!$isPublic && !str_starts_with($key, 'App.') && !str_starts_with($key, '.App.')) {
                // The synthetic `App` root segment is stripped again by the pattern match
                // below; it only exists so the pattern extracts the right context segment.
                $key = 'App.'.$key;
            }
            $routingKey = 1 === preg_match($pattern, $key, $matches) ? ($matches[1] ?? $key) : $key;
            $routingKey = mb_trim($routingKey, '.');
            $routingKey = ($isPublic ? '' : 'internal.').$routingKey;
        } else {
            $routingKey = $key;
        }

        $routingKey = (null !== $prefix && '' !== $prefix && !str_starts_with($routingKey, $prefix.'.') ? $prefix.'.' : '').$routingKey;
        $routingKey .= (null !== $suffix && '' !== $suffix && !str_ends_with($routingKey, '.'.$suffix) ? '.'.$suffix : '');

        $this->key = $routingKey;
    }

    public function __toString(): string
    {
        return $this->key;
    }

    public static function createForDirectTransport(mixed $data, ?string $prefix = null, ?string $suffix = null): self
    {
        return new self($data, '/^[^.]+\.(\w+)\./', $prefix, $suffix);
    }

    public static function createForTopicTransport(mixed $data, ?string $prefix = null, ?string $suffix = null): self
    {
        return new self($data, '/^[^.]+\.(.+)/', $prefix, $suffix);
    }
}
