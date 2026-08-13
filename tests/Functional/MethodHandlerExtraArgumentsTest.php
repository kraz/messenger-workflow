<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Functional;

use Kraz\MessengerWorkflow\Tests\Fixture\Handler\ExtraServicesCommandController;
use Kraz\MessengerWorkflow\Tests\Fixture\Handler\ExtraServicesEventHandlers;
use Kraz\MessengerWorkflow\Tests\Fixture\Handler\MoreExtraServicesEventHandlers;
use Kraz\MessengerWorkflow\Tests\Fixture\Message\ExtraServicesCommand;
use Kraz\MessengerWorkflow\Tests\Fixture\Message\ExtraServicesEvent;
use Kraz\MessengerWorkflow\Tests\Fixture\MessageRecorder;
use Kraz\MessengerWorkflow\Tests\Support\WorkflowKernelTestCase;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Handler\HandlerDescriptor;
use Symfony\Component\Messenger\Handler\HandlersLocatorInterface;

/**
 * Spec: handler methods may declare extra parameters after the message — they are
 * resolved from the container (nullable ones degrade to null when no service exists,
 * defaults are kept), so controller-style classes can host handler methods without
 * constructor injection.
 */
final class MethodHandlerExtraArgumentsTest extends WorkflowKernelTestCase
{
    /**
     * @return list<HandlerDescriptor>
     */
    private function descriptors(string $busId, object $message): array
    {
        /** @var HandlersLocatorInterface $locator */
        $locator = self::getContainer()->get($busId.'.messenger.handlers_locator');

        return iterator_to_array($locator->getHandlers(new Envelope($message)), false);
    }

    public function testExtraArgumentsAreResolvedFromTheContainer(): void
    {
        $descriptors = $this->descriptors('command.bus', $command = new ExtraServicesCommand('p1'));
        self::assertCount(1, $descriptors);

        // no-service: the nullable NotAService parameter has no service and resolves to
        // null; 42: the scalar default is preserved.
        self::assertSame('extra-command-result:p1:no-service:42', $descriptors[0]->getHandler()($command));

        /** @var MessageRecorder $recorder */
        $recorder = self::getContainer()->get(MessageRecorder::class);
        self::assertSame([ExtraServicesCommandController::class.'::handle'], $recorder->handlersFor($command));
    }

    public function testWrappedHandlerIsRegisteredOnItsBusOnly(): void
    {
        self::assertCount(1, $this->descriptors('command.bus', new ExtraServicesCommand()));
        self::assertCount(0, $this->descriptors('query.bus', new ExtraServicesCommand()));
        self::assertCount(0, $this->descriptors('event.bus', new ExtraServicesCommand()));
    }

    public function testWrappedEventHandlersKeepDistinctDescriptorNames(): void
    {
        $descriptors = $this->descriptors('event.bus', $event = new ExtraServicesEvent());
        self::assertCount(2, $descriptors);

        // HandleMessageMiddleware skips a handler whose descriptor name it has already
        // seen for the envelope — identical names would silently drop the second handler.
        $names = array_map(static fn (HandlerDescriptor $descriptor): string => $descriptor->getName(), $descriptors);
        self::assertCount(2, array_unique($names));

        foreach ($descriptors as $descriptor) {
            $descriptor->getHandler()($event);
        }

        /** @var MessageRecorder $recorder */
        $recorder = self::getContainer()->get(MessageRecorder::class);
        self::assertEqualsCanonicalizing([
            ExtraServicesEventHandlers::class.'::onExtraServicesEvent',
            MoreExtraServicesEventHandlers::class.'::onExtraServicesEvent',
        ], $recorder->handlersFor($event));
    }
}
