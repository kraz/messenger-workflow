<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Messenger;

readonly class RoutingKey implements \Stringable
{
    private string $key;

    public function __construct(mixed $data, string $pattern, ?string $prefix = null, ?string $suffix = null)
    {
        $key = \is_object($data) ? $data::class : (string) $data;
        if (str_contains($key, '\\')) {
            $key = str_replace('\\', '.', $key);
            $isPublic = str_starts_with($key, 'Contracts.') || str_starts_with($key, '.Contracts.');
            if (!$isPublic && !str_starts_with('App.', $key) && !str_starts_with('.App.', $key)) {
                $key = 'App.'.$key;
            }
            $routingKey = preg_match($pattern, $key, $matches) ? ($matches[1] ?? $key) : $key;
            $routingKey = mb_trim($routingKey, '.');
            $routingKey = ($isPublic ? '' : 'internal.').$routingKey;
        } else {
            $routingKey = $key;
        }

        $routingKey = ($prefix && !str_starts_with($routingKey, $prefix.'.') ? $prefix.'.' : '').$routingKey;
        $routingKey .= ($suffix && !str_ends_with($routingKey, '.'.$suffix) ? '.'.$suffix : '');

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
