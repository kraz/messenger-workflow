<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Infrastructure\DependencyInjection\Compiler;

use Kraz\MessengerWorkflow\Infrastructure\Messenger\Handler\HandlerMethodInvoker;
use Symfony\Component\DependencyInjection\Attribute\Autowire;
use Symfony\Component\DependencyInjection\Attribute\Target;
use Symfony\Component\DependencyInjection\Compiler\CompilerPassInterface;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\ContainerInterface;
use Symfony\Component\DependencyInjection\Exception\RuntimeException;
use Symfony\Component\DependencyInjection\Reference;
use Symfony\Component\DependencyInjection\TypedReference;

/**
 * Wraps handler methods declaring extra parameters after the message (tagged
 * "messenger_workflow.method_handler" by the attribute autoconfiguration) into
 * HandlerMethodInvoker services and tags those as the actual messenger handlers.
 *
 * The extra arguments are emitted as TypedReferences carrying the parameter name and
 * any #[Autowire]/#[Target] attributes, so Symfony's AutowirePass resolves them with
 * full autowiring semantics even though the invoker definition itself is not autowired.
 *
 * Must run after ResolveInstanceofConditionalsPass (which materializes autoconfigured
 * tags) and before Symfony's MessengerPass (which consumes "messenger.message_handler").
 */
final class MethodHandlerArgumentsPass implements CompilerPassInterface
{
    private const string TAG = 'messenger_workflow.method_handler';

    public function process(ContainerBuilder $container): void
    {
        foreach ($container->findTaggedServiceIds(self::TAG, true) as $serviceId => $tags) {
            $definition = $container->getDefinition($serviceId);
            $definition->clearTag(self::TAG);

            $class = $container->getParameterBag()->resolveValue($definition->getClass()) ?? $serviceId;
            $reflection = \is_string($class) && '' !== $class ? $container->getReflectionClass($class, false) : null;
            if (null === $reflection) {
                throw new RuntimeException(\sprintf('Invalid handler service "%s": class "%s" cannot be loaded.', $serviceId, \is_scalar($class) ? (string) $class : get_debug_type($class)));
            }

            foreach ($tags as $tag) {
                if (!\is_array($tag)) {
                    continue;
                }
                $this->registerInvoker($container, $serviceId, $reflection, $tag);
            }
        }
    }

    /**
     * @param \ReflectionClass<object> $reflection
     * @param array<mixed>             $tag
     */
    private function registerInvoker(ContainerBuilder $container, string $serviceId, \ReflectionClass $reflection, array $tag): void
    {
        $method = \is_string($tag['method'] ?? null) && '' !== $tag['method'] ? $tag['method'] : '__invoke';
        if (!$reflection->hasMethod($method)) {
            throw new RuntimeException(\sprintf('Invalid handler service "%s": method "%s::%s()" does not exist.', $serviceId, $reflection->getName(), $method));
        }

        $handlerDescription = \sprintf('"%s::%s()"', $reflection->getName(), $method);
        $parameters = $reflection->getMethod($method)->getParameters();

        $extraArguments = [];
        foreach (\array_slice($parameters, 1) as $parameter) {
            $extraArguments[] = $this->resolveExtraArgument($parameter, $serviceId, $handlerDescription);
        }

        $invokerId = '.messenger_workflow.method_handler.'.$serviceId.'::'.$method;
        if (!$container->hasDefinition($invokerId)) {
            $container->register($invokerId, HandlerMethodInvoker::class)
                ->setArguments([new Reference($serviceId), $method, $extraArguments]);
        }
        $invoker = $container->getDefinition($invokerId);

        $tagAttributes = [];
        foreach ($tag as $attribute => $value) {
            if (\is_string($attribute) && null !== $value) {
                $tagAttributes[$attribute] = $value;
            }
        }
        unset($tagAttributes['method'], $tagAttributes['handles']);
        // Unique descriptor name per wrapped method: every invoker shares the
        // HandlerMethodInvoker::__invoke callable, whose bare name would collide in
        // HandleMessageMiddleware's already-handled bookkeeping.
        $fromTransport = \is_string($tag['from_transport'] ?? null) && '' !== $tag['from_transport'] ? $tag['from_transport'] : null;
        $tagAttributes['alias'] ??= $serviceId.'::'.$method.(null !== $fromTransport ? '#'.$fromTransport : '');

        foreach ($this->handledClasses($serviceId, $handlerDescription, $parameters, $tag) as $handledClass) {
            $invoker->addTag('messenger.message_handler', ['handles' => $handledClass] + $tagAttributes);
        }
    }

    /**
     * The wrapper's own __invoke(object $message) hides the message type, so the
     * "handles" tag attribute is always made explicit: taken from the attribute when
     * set, guessed from the method's first parameter type otherwise.
     *
     * @param list<\ReflectionParameter> $parameters
     * @param array<mixed>               $tag
     *
     * @return non-empty-list<string>
     */
    private function handledClasses(string $serviceId, string $handlerDescription, array $parameters, array $tag): array
    {
        if (\is_string($tag['handles'] ?? null) && '' !== $tag['handles']) {
            return [$tag['handles']];
        }

        $type = ($parameters[0] ?? null)?->getType();
        $handles = [];
        foreach ($type instanceof \ReflectionUnionType ? $type->getTypes() : [$type] as $parameterType) {
            if ($parameterType instanceof \ReflectionNamedType && !$parameterType->isBuiltin()) {
                $handles[] = $parameterType->getName();
            }
        }
        if ([] === $handles) {
            throw new RuntimeException(\sprintf('Invalid handler service "%s": the handled message class cannot be guessed from the first argument of %s — type-hint it with the message class or set the "handles" attribute option.', $serviceId, $handlerDescription));
        }

        return $handles;
    }

    private function resolveExtraArgument(\ReflectionParameter $parameter, string $serviceId, string $handlerDescription): mixed
    {
        $attributes = [];
        foreach ($parameter->getAttributes(Autowire::class, \ReflectionAttribute::IS_INSTANCEOF) as $attribute) {
            $attributes[] = $attribute->newInstance();
        }
        foreach ($parameter->getAttributes(Target::class) as $attribute) {
            $attributes[] = $attribute->newInstance();
        }

        $type = $parameter->getType();
        $typeName = $type instanceof \ReflectionNamedType && !$type->isBuiltin() ? $type->getName() : null;
        if ('self' === $typeName || 'parent' === $typeName || 'static' === $typeName) {
            $typeName = $parameter->getDeclaringClass()?->getName();
        }

        if (null === $typeName && [] === $attributes) {
            if ($parameter->isDefaultValueAvailable()) {
                return $parameter->getDefaultValue();
            }

            throw new RuntimeException(\sprintf('Invalid handler service "%s": cannot resolve argument "$%s" of %s — extra handler-method arguments must be autowirable (a class/interface type-hint or an #[Autowire] attribute) or declare a default value.', $serviceId, $parameter->name, $handlerDescription));
        }

        // Nullable services degrade to null when absent; required ones fail at compile
        // time. The reference position in the arguments list is preserved either way.
        $invalidBehavior = $parameter->allowsNull() ? ContainerInterface::NULL_ON_INVALID_REFERENCE : ContainerInterface::EXCEPTION_ON_INVALID_REFERENCE;

        return new TypedReference($typeName ?? '?', $typeName ?? 'mixed', $invalidBehavior, $parameter->name, $attributes);
    }
}
