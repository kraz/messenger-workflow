<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Unit\Messenger;

use Contracts\Demo\Command\DoSomethingCommand;
use Contracts\Demo\Event\SomethingHappened;
use Kraz\MessengerWorkflow\Messenger\RoutingKey;
use Kraz\MessengerWorkflow\Tests\Fixture\Message\TestCommand;
use Kraz\MessengerWorkflow\Tests\Fixture\Message\TestEvent;
use PHPUnit\Framework\TestCase;

/**
 * Spec: RabbitMQ integration — commands/queries use direct exchanges, events use topic
 * exchanges; public contract messages route by bounded-context segment, internal messages
 * are prefixed with "internal.".
 */
final class RoutingKeyTest extends TestCase
{
    public function testPublicContractCommandRoutesByBoundedContextOnDirectExchange(): void
    {
        $key = RoutingKey::createForDirectTransport(new DoSomethingCommand(), 'commands');

        self::assertSame('commands.Demo', (string) $key);
    }

    public function testPublicContractEventKeepsFullPathOnTopicExchange(): void
    {
        $key = RoutingKey::createForTopicTransport(new SomethingHappened(), 'events');

        self::assertSame('events.Demo.Event.SomethingHappened', (string) $key);
    }

    public function testInternalCommandIsPrefixedWithInternalAndBoundedContext(): void
    {
        // Fixture namespace Kraz\MessengerWorkflow\... -> bounded context segment "Kraz"
        $key = RoutingKey::createForDirectTransport(new TestCommand(), 'commands');

        self::assertSame('commands.internal.Kraz', (string) $key);
    }

    public function testInternalEventKeepsFullPathWithInternalPrefix(): void
    {
        $key = RoutingKey::createForTopicTransport(new TestEvent(), 'events');

        self::assertSame(
            'events.internal.Kraz.MessengerWorkflow.Tests.Fixture.Message.TestEvent',
            (string) $key,
        );
    }

    public function testScalarValuePassesThroughUntouched(): void
    {
        self::assertSame('foo', (string) RoutingKey::createForDirectTransport('foo'));
        self::assertSame('commands.foo', (string) RoutingKey::createForDirectTransport('foo', 'commands'));
    }

    public function testSuffixIsAppendedOnce(): void
    {
        self::assertSame(
            'events.internal.TestApp.#',
            (string) RoutingKey::createForTopicTransport('internal.TestApp', 'events', '#'),
        );
        self::assertSame(
            'events.internal.TestApp.#',
            (string) RoutingKey::createForTopicTransport('internal.TestApp.#', 'events', '#'),
        );
    }

    public function testPrefixIsNotDuplicated(): void
    {
        self::assertSame(
            'commands.TestApp',
            (string) RoutingKey::createForDirectTransport('commands.TestApp', 'commands'),
        );
    }

    /**
     * Regression test: str_starts_with('App.', $key) had
     * needle/haystack swapped, so classes under the App\ namespace were double-prefixed
     * and every internal App\* message collapsed to routing key "internal.App".
     */
    public function testAppNamespaceClassRoutesByItsModuleSegment(): void
    {
        if (!class_exists(\App\ModuleX\SampleCommand::class, false)) {
            eval('namespace App\ModuleX; class SampleCommand {}');
        }

        $key = RoutingKey::createForDirectTransport(new \App\ModuleX\SampleCommand(), 'commands');

        self::assertSame('commands.internal.ModuleX', (string) $key);
    }
}
