<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Infrastructure\Messenger;

use Kraz\MessengerWorkflow\Application\Attribute\MessageRoute;

/**
 * Resolves the route (the extra routing-key segment) of a command or query class.
 *
 * Sources, in order: the compiled `message class → route` map built by
 * ConfigureTransportsPass from the `messages` lists of the routed queue bindings, then
 * a #[MessageRoute] attribute on the class. Both are looked up along the class lineage:
 * the class itself, its parents (nearest first), then its interfaces. Interfaces naming
 * different routes for one class are ambiguous and rejected. Results are cached per
 * class — the resolver runs on every dispatch and every outbox relay.
 */
final class MessageRouteResolver
{
    /**
     * @var array<string, string|null>
     */
    private array $resolved = [];

    /**
     * @param array<string, string> $routes message class (or parent class / interface) → route name
     */
    public function __construct(
        private readonly array $routes = [],
    ) {
    }

    /**
     * @return array<string, string>
     */
    public function getRoutes(): array
    {
        return $this->routes;
    }

    public function resolve(object|string $message): ?string
    {
        $class = \is_object($message) ? $message::class : $message;
        if (\array_key_exists($class, $this->resolved)) {
            return $this->resolved[$class];
        }

        return $this->resolved[$class] = $this->doResolve($class);
    }

    private function doResolve(string $class): ?string
    {
        if (!class_exists($class) && !interface_exists($class)) {
            return null;
        }

        [$chain, $interfaces] = self::lineage($class);

        return $this->match($class, $chain, $interfaces, fn (string $candidate): ?string => $this->routes[$candidate] ?? null, 'configured for')
            ?? $this->match($class, $chain, $interfaces, self::attributeRoute(...), 'declared by #[MessageRoute] on');
    }

    /**
     * @param list<class-string>             $chain      the class and its parents, nearest first
     * @param list<class-string>             $interfaces
     * @param \Closure(class-string): ?string $lookup
     */
    private function match(string $class, array $chain, array $interfaces, \Closure $lookup, string $sourceDescription): ?string
    {
        foreach ($chain as $candidate) {
            $route = $lookup($candidate);
            if (null !== $route) {
                return $route;
            }
        }

        $matches = [];
        foreach ($interfaces as $interface) {
            $route = $lookup($interface);
            if (null !== $route) {
                $matches[$interface] = $route;
            }
        }
        if (\count(array_unique($matches)) > 1) {
            $described = [];
            foreach ($matches as $interface => $route) {
                $described[] = \sprintf('"%s" (route "%s")', $interface, $route);
            }
            throw new \LogicException(\sprintf('The route of message "%s" is ambiguous: different routes are %s its interfaces %s.', $class, $sourceDescription, implode(', ', $described)));
        }

        $route = reset($matches);

        return false === $route ? null : $route;
    }

    /**
     * @param class-string $class
     *
     * @return array{list<class-string>, list<class-string>}
     */
    private static function lineage(string $class): array
    {
        $parents = class_parents($class);
        $interfaces = class_implements($class);

        return [
            [$class, ...array_values(false === $parents ? [] : $parents)],
            array_values(false === $interfaces ? [] : $interfaces),
        ];
    }

    /**
     * @param class-string $class
     */
    private static function attributeRoute(string $class): ?string
    {
        $attributes = new \ReflectionClass($class)->getAttributes(MessageRoute::class);

        return [] === $attributes ? null : $attributes[0]->newInstance()->route;
    }
}
