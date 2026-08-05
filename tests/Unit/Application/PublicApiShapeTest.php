<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Unit\Application;

use Kraz\MessengerWorkflow\Application\Attribute\AsCommandHandler;
use Kraz\MessengerWorkflow\Application\Attribute\AsEventHandler;
use Kraz\MessengerWorkflow\Application\Attribute\AsQueryHandler;
use Kraz\MessengerWorkflow\Application\CommandInterface;
use Kraz\MessengerWorkflow\Application\Exception\PendingOutboxMessageException;
use Kraz\MessengerWorkflow\Application\Exception\TaskFailedException;
use Kraz\MessengerWorkflow\Application\Exception\TaskTimeOutException;
use Kraz\MessengerWorkflow\Application\Messenger\CommandBusInterface;
use Kraz\MessengerWorkflow\Application\Messenger\EventBusInterface;
use Kraz\MessengerWorkflow\Application\Messenger\QueryBusInterface;
use Kraz\MessengerWorkflow\Application\QueryInterface;
use Kraz\MessengerWorkflow\Domain\DomainEventInterface;
use Kraz\MessengerWorkflow\Domain\OutboxBusInterface;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Attribute\AsMessageHandler;
use Symfony\Component\Messenger\Exception\TransportException;

/**
 * Freezes the public API surface of the package — see UPGRADE-0.3.md for what 0.3
 * preserves and what it deliberately changes.
 * A failure here means an unintended breaking change to the package contract.
 */
final class PublicApiShapeTest extends TestCase
{
    public function testCommandBusDispatchSignature(): void
    {
        $method = new \ReflectionMethod(CommandBusInterface::class, 'dispatch');
        $parameters = $method->getParameters();

        self::assertCount(2, $parameters);
        self::assertSame('void', (string) $method->getReturnType());

        self::assertSame('command', $parameters[0]->getName());
        self::assertSame('object', (string) $parameters[0]->getType());
        self::assertFalse($parameters[0]->isPassedByReference());

        self::assertSame('taskId', $parameters[1]->getName());
        self::assertSame('?string', (string) $parameters[1]->getType());
        self::assertTrue($parameters[1]->isPassedByReference(), 'The taskId argument must be by-reference');
        self::assertTrue($parameters[1]->isDefaultValueAvailable());
        self::assertNull($parameters[1]->getDefaultValue());
    }

    public function testCommandBusHasNoDispatchAsyncMethod(): void
    {
        // B2: dispatchAsync() is removed — dispatch($command, $taskId) replaces it.
        self::assertFalse(method_exists(CommandBusInterface::class, 'dispatchAsync'));
    }

    public function testCommandBusAwaitSignature(): void
    {
        $method = new \ReflectionMethod(CommandBusInterface::class, 'await');
        $parameters = $method->getParameters();

        self::assertCount(2, $parameters);
        self::assertSame('void', (string) $method->getReturnType());
        self::assertSame('taskId', $parameters[0]->getName());
        self::assertSame('string', (string) $parameters[0]->getType());
        self::assertSame('timeout', $parameters[1]->getName());
        self::assertSame('?int', (string) $parameters[1]->getType());
        self::assertNull($parameters[1]->getDefaultValue());
    }

    public function testQueryBusKeepsItsOriginalApi(): void
    {
        $ask = new \ReflectionMethod(QueryBusInterface::class, 'ask');
        self::assertSame('mixed', (string) $ask->getReturnType());
        self::assertSame(['query', 'timeout'], array_map(static fn (\ReflectionParameter $p): string => $p->getName(), $ask->getParameters()));
        self::assertSame('object', (string) $ask->getParameters()[0]->getType());
        self::assertSame('?int', (string) $ask->getParameters()[1]->getType());

        $askAsync = new \ReflectionMethod(QueryBusInterface::class, 'askAsync');
        self::assertSame('string', (string) $askAsync->getReturnType());
        self::assertSame(['query'], array_map(static fn (\ReflectionParameter $p): string => $p->getName(), $askAsync->getParameters()));

        $await = new \ReflectionMethod(QueryBusInterface::class, 'await');
        self::assertSame('mixed', (string) $await->getReturnType());
        self::assertSame(['taskId', 'timeout'], array_map(static fn (\ReflectionParameter $p): string => $p->getName(), $await->getParameters()));
    }

    public function testEventBusPublishSignature(): void
    {
        $method = new \ReflectionMethod(EventBusInterface::class, 'publish');
        $parameters = $method->getParameters();

        self::assertCount(1, $parameters);
        self::assertSame('void', (string) $method->getReturnType());
        self::assertSame('object', (string) $parameters[0]->getType());
    }

    public function testDomainContracts(): void
    {
        self::assertTrue(interface_exists(CommandInterface::class));
        self::assertTrue(interface_exists(QueryInterface::class));

        $publish = new \ReflectionMethod(OutboxBusInterface::class, 'publish');
        self::assertSame('void', (string) $publish->getReturnType());
        self::assertSame(DomainEventInterface::class, (string) $publish->getParameters()[0]->getType());

        self::assertTrue(method_exists(DomainEventInterface::class, 'withMetadata'));
        self::assertTrue(method_exists(DomainEventInterface::class, 'withoutMetadata'));
        self::assertTrue((new \ReflectionMethod(DomainEventInterface::class, 'withMetadata'))->getParameters()[0]->isVariadic());
    }

    public function testExceptionHierarchy(): void
    {
        self::assertTrue(is_subclass_of(TaskFailedException::class, \RuntimeException::class));
        self::assertTrue(is_subclass_of(TaskTimeOutException::class, TransportException::class));
        self::assertTrue(is_subclass_of(PendingOutboxMessageException::class, \LogicException::class));
    }

    /**
     * @return iterable<string, array{class-string}>
     */
    public static function handlerAttributeProvider(): iterable
    {
        yield 'command' => [AsCommandHandler::class];
        yield 'query' => [AsQueryHandler::class];
        yield 'event' => [AsEventHandler::class];
    }

    /**
     * @param class-string $attributeClass
     */
    #[DataProvider('handlerAttributeProvider')]
    public function testHandlerAttributesExtendAsMessageHandler(string $attributeClass): void
    {
        self::assertTrue(is_subclass_of($attributeClass, AsMessageHandler::class));

        $attributes = new \ReflectionClass($attributeClass)->getAttributes(\Attribute::class);
        self::assertCount(1, $attributes);

        /** @var \Attribute $attribute */
        $attribute = $attributes[0]->newInstance();
        self::assertSame(\Attribute::TARGET_CLASS | \Attribute::TARGET_METHOD | \Attribute::IS_REPEATABLE, $attribute->flags);
    }
}
