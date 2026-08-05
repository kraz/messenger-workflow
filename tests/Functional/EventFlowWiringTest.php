<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Functional;

use Kraz\MessengerWorkflow\Application\Messenger\EventBusInterface;
use Kraz\MessengerWorkflow\Domain\OutboxBusInterface;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\EventBus;
use Kraz\MessengerWorkflow\Infrastructure\Messenger\OutboxBus;
use Kraz\MessengerWorkflow\Tests\Support\WorkflowKernelTestCase;

/**
 * Event-flow container wiring: the EventBus behind EventBusInterface and the per-context
 * outbox buses registered from `messenger_workflow.messenger.outbox_buses`.
 */
final class EventFlowWiringTest extends WorkflowKernelTestCase
{
    public function testEventBusInterfaceResolvesToTheWorkflowEventBus(): void
    {
        self::assertInstanceOf(EventBus::class, self::getContainer()->get(EventBusInterface::class));
    }

    public function testConfiguredOutboxBusesAreRegistered(): void
    {
        $container = self::getContainer();

        self::assertInstanceOf(OutboxBus::class, $container->get('messenger_workflow.outbox_bus.app'));
        // A single configured context also claims the plain interface alias.
        self::assertSame($container->get('messenger_workflow.outbox_bus.app'), $container->get(OutboxBusInterface::class));
    }
}
