<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\TestKernel;

use Kraz\MessengerWorkflow\Tests\Fixture\Handler\InvalidMethodDeclaringHandler;
use Symfony\Component\DependencyInjection\Loader\Configurator\ContainerConfigurator;

/**
 * Registers the invalid fixture whose method-level attribute declares a "method",
 * which must make container compilation fail with a LogicException.
 */
final class InvalidMethodHandlerKernel extends TestKernel
{
    protected function registerFixtureServices(ContainerConfigurator $container): void
    {
        $container->services()
            ->set(InvalidMethodDeclaringHandler::class)
            ->autowire()
            ->autoconfigure();
    }
}
