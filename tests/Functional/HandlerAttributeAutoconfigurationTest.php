<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Functional;

use Kraz\MessengerWorkflow\Tests\Fixture\Message\DuplicatedQuery;
use Kraz\MessengerWorkflow\Tests\Fixture\Message\TestCommand;
use Kraz\MessengerWorkflow\Tests\Fixture\Message\TestEvent;
use Kraz\MessengerWorkflow\Tests\Fixture\Message\TestQuery;
use Kraz\MessengerWorkflow\Tests\Fixture\Message\TransportScopedEvent;
use Kraz\MessengerWorkflow\Tests\Fixture\Message\UnhandledEvent;
use Kraz\MessengerWorkflow\Tests\Support\WorkflowKernelTestCase;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Handler\HandlersLocatorInterface;
use Symfony\Component\Messenger\Stamp\ReceivedStamp;

/**
 * Spec: message type enforcement — commands/queries exactly one handler, events zero or more.
 * The #[AsCommandHandler]/#[AsQueryHandler]/#[AsEventHandler] attributes must register
 * handlers on their respective bus only.
 */
final class HandlerAttributeAutoconfigurationTest extends WorkflowKernelTestCase
{
    private function handlerCount(string $busId, object $message, ?string $receivedFrom = null): int
    {
        /** @var HandlersLocatorInterface $locator */
        $locator = self::getContainer()->get($busId.'.messenger.handlers_locator');
        $stamps = null !== $receivedFrom ? [new ReceivedStamp($receivedFrom)] : [];

        return iterator_count($locator->getHandlers(new Envelope($message, $stamps)));
    }

    public function testCommandHandlerIsRegisteredOnCommandBusOnly(): void
    {
        self::assertSame(1, $this->handlerCount('command.bus', new TestCommand()));
        self::assertSame(0, $this->handlerCount('query.bus', new TestCommand()));
        self::assertSame(0, $this->handlerCount('event.bus', new TestCommand()));
    }

    public function testQueryHandlerIsRegisteredOnQueryBusOnly(): void
    {
        self::assertSame(1, $this->handlerCount('query.bus', new TestQuery()));
        self::assertSame(0, $this->handlerCount('command.bus', new TestQuery()));
    }

    public function testEventsSupportMultipleHandlers(): void
    {
        // Spec: "Events can have zero or more handlers"
        self::assertSame(2, $this->handlerCount('event.bus', new TestEvent()));
        self::assertSame(0, $this->handlerCount('event.bus', new UnhandledEvent()));
    }

    public function testMethodLevelAttributesRegisterEachMethodAsHandler(): void
    {
        // DuplicatedQueryHandlers declares two #[AsQueryHandler] methods for the same query
        self::assertSame(2, $this->handlerCount('query.bus', new DuplicatedQuery()));
    }

    public function testFromTransportRestrictsHandlerToItsTransport(): void
    {
        self::assertSame(1, $this->handlerCount('event.bus', new TransportScopedEvent(), 'app_events'));
        self::assertSame(0, $this->handlerCount('event.bus', new TransportScopedEvent(), 'another_transport'));
    }
}
