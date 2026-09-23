<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Unit\Infrastructure\Messenger;

use Contracts\Demo\Command\DoSomethingCommand;
use Contracts\Demo\Event\SomethingHappened;
use Contracts\Demo\Query\GetSomethingQuery;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\AmqpStampFactory;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\MessageRouteResolver;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\Stamp\MessageIdStamp;
use Kraz\MessengerWorkflow\Tests\Fixture\Message\Routing\AttributeRoutedCommand;
use Kraz\MessengerWorkflow\Tests\Fixture\Message\Routing\ConcreteRoutedCommand;
use Kraz\MessengerWorkflow\Tests\Fixture\Message\TestCommand;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Envelope;

/**
 * Spec: a routed command/query class gets the context key plus its route segment
 * (`commands.internal.<Ctx>.<route>` / `commands.<Ctx>.<route>` for public contracts);
 * an unrouted one keeps the plain context key; events are never routed.
 */
final class AmqpStampFactoryTest extends TestCase
{
    private function routingKey(AmqpStampFactory $factory, object $message): ?string
    {
        return $factory->createForEnvelope(new Envelope($message, [new MessageIdStamp('id-1')]))?->getRoutingKey();
    }

    public function testUnroutedMessagesKeepTheContextKeys(): void
    {
        $factory = new AmqpStampFactory();

        self::assertSame('commands.Demo', $this->routingKey($factory, new DoSomethingCommand()));
        self::assertSame('commands.internal.Kraz', $this->routingKey($factory, new TestCommand()));
        self::assertSame('queries.Demo', $this->routingKey($factory, new GetSomethingQuery()));
        self::assertSame('events.Demo.Event.SomethingHappened', $this->routingKey($factory, new SomethingHappened()));
    }

    public function testARoutedPublicContractCommandGetsTheRouteSegment(): void
    {
        $factory = new AmqpStampFactory(new MessageRouteResolver([DoSomethingCommand::class => 'planning']));

        self::assertSame('commands.Demo.planning', $this->routingKey($factory, new DoSomethingCommand()));
        self::assertSame('queries.Demo', $this->routingKey($factory, new GetSomethingQuery()), 'Other classes are untouched');
    }

    public function testARoutedInternalCommandGetsTheRouteSegment(): void
    {
        $factory = new AmqpStampFactory(new MessageRouteResolver([TestCommand::class => 'planning']));

        self::assertSame('commands.internal.Kraz.planning', $this->routingKey($factory, new TestCommand()));
    }

    public function testARoutedQueryGetsTheRouteSegment(): void
    {
        $factory = new AmqpStampFactory(new MessageRouteResolver([GetSomethingQuery::class => 'reports']));

        self::assertSame('queries.Demo.reports', $this->routingKey($factory, new GetSomethingQuery()));
    }

    public function testTheRouteIsResolvedAlongTheLineageAndFromTheAttribute(): void
    {
        $factory = new AmqpStampFactory(new MessageRouteResolver([\Kraz\MessengerWorkflow\Tests\Fixture\Message\Routing\AbstractRoutedCommand::class => 'bulk']));

        self::assertSame('commands.internal.Kraz.bulk', $this->routingKey($factory, new ConcreteRoutedCommand()));
        self::assertSame('commands.internal.Kraz.slow', $this->routingKey($factory, new AttributeRoutedCommand()));
    }

    public function testTheMessageIdBecomesTheAmqpMessageIdAttribute(): void
    {
        $stamp = new AmqpStampFactory()->createForEnvelope(new Envelope(new DoSomethingCommand(), [new MessageIdStamp('id-7')]));

        self::assertNotNull($stamp);
        self::assertSame('id-7', $stamp->getAttributes()['message_id'] ?? null);
    }
}
