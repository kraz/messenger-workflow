<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Functional;

use Kraz\MessengerWorkflow\MessengerWorkflowBundle;
use Kraz\MessengerWorkflow\Tests\Support\WorkflowKernelTestCase;

final class BundleWiringTest extends WorkflowKernelTestCase
{
    public function testKernelBootsWithTheBundleRegistered(): void
    {
        self::bootKernel();

        $kernel = self::$kernel;
        self::assertNotNull($kernel);

        $bundles = $kernel->getBundles();
        self::assertArrayHasKey('MessengerWorkflowBundle', $bundles);
        self::assertInstanceOf(MessengerWorkflowBundle::class, $bundles['MessengerWorkflowBundle']);
    }

    public function testExtensionAliasIsMessengerWorkflow(): void
    {
        $extension = new MessengerWorkflowBundle()->getContainerExtension();

        self::assertNotNull($extension);
        self::assertSame('messenger_workflow', $extension->getAlias());
    }
}
