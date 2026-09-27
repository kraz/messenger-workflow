<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Functional;

use Kraz\MessengerWorkflow\Tests\Support\WorkflowKernelTestCase;
use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\Attributes\RequiresPhpExtension;
use Symfony\Bundle\FrameworkBundle\Console\Application;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Tester\CommandTester;

/**
 * messenger:prune-inbox is registered and reaches the inbox transports by name; with no
 * transport argument it covers every inbox transport and skips the others.
 */
final class PruneInboxCommandWiringTest extends WorkflowKernelTestCase
{
    private function tester(): CommandTester
    {
        $kernel = self::bootKernel();

        return new CommandTester(new Application($kernel)->find('messenger:prune-inbox'));
    }

    public function testANamedInboxTransportIsReachable(): void
    {
        $tester = $this->tester();

        self::assertSame(Command::SUCCESS, $tester->execute(['transports' => ['beta_events'], '--dry-run' => true]));
        self::assertStringContainsString('zz_events_inbox_index', $tester->getDisplay());
    }

    public function testANonInboxTransportIsRefused(): void
    {
        $tester = $this->tester();

        self::assertSame(Command::FAILURE, $tester->execute(['transports' => ['app_outbox']]));
        self::assertStringContainsString('"app_outbox" is not an inbox transport', $tester->getDisplay());
    }

    #[Group('postgres')]
    #[RequiresPhpExtension('pdo_pgsql')]
    public function testEveryInboxTransportIsCoveredByDefault(): void
    {
        $tester = $this->tester();

        self::assertSame(Command::SUCCESS, $tester->execute(['--dry-run' => true]));

        $display = $tester->getDisplay();
        foreach (['app_commands', 'app_events', 'beta_events', 'gamma_events'] as $inbox) {
            self::assertMatchesRegularExpression('/^\s*'.$inbox.'\s/m', $display);
        }
        foreach (['commands', 'app_outbox', 'app_commands_failures'] as $other) {
            self::assertDoesNotMatchRegularExpression('/^\s*'.$other.'\s/m', $display);
        }
    }
}
