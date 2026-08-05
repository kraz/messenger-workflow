<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Support;

use Symfony\Component\DependencyInjection\Compiler\CompilerPassInterface;
use Symfony\Component\DependencyInjection\ContainerBuilder;

/**
 * Keeps selected messenger services public so functional tests can inspect the
 * compiled wiring (they are otherwise inlined and removed).
 */
final class ExposeHandlersLocatorsPass implements CompilerPassInterface
{
    private const array SERVICE_IDS = [
        'command.bus.messenger.handlers_locator',
        'query.bus.messenger.handlers_locator',
        'event.bus.messenger.handlers_locator',
        'messenger.senders_locator',
        'messenger_workflow.message_id_middleware',
    ];

    private const array ALIAS_IDS = [
        'messenger.default_serializer',
        'messenger_workflow.result_storage',
        \Kraz\MessengerWorkflow\Application\Task\ResultStorageInterface::class,
    ];

    public function process(ContainerBuilder $container): void
    {
        foreach (self::SERVICE_IDS as $serviceId) {
            if ($container->hasDefinition($serviceId)) {
                $container->getDefinition($serviceId)->setPublic(true);
            }
        }

        foreach (self::ALIAS_IDS as $aliasId) {
            if ($container->hasAlias($aliasId)) {
                $container->getAlias($aliasId)->setPublic(true);
            }
        }
    }
}
