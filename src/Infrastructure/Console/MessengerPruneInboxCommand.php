<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Infrastructure\Console;

use Kraz\MessengerWorkflow\Infrastructure\Doctrine\Inbox\InboxTransport;
use Psr\Container\ContainerInterface;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;

/**
 * Deletes processed inbox rows: the dedup index entries (`<table_name>_index`) whose
 * message was acked or permanently rejected before the retention cut-off. The inbox row
 * itself is already gone at that point — only its index entry remains, and it keeps
 * dropping broker redeliveries of the message UUID for as long as it exists.
 *
 * The retention is counted in whole days on purpose: it must lie far beyond every
 * redelivery and retry window (redeliver_timeout, the flow retry budgets, a broker
 * holding unacked deliveries of a stopped receiver), which are seconds to minutes.
 * An entry pruned too early lets a late redelivery through, and the message is handled
 * a second time.
 */
#[AsCommand(name: 'messenger:prune-inbox', description: 'Delete processed inbox dedup entries older than the retention period')]
class MessengerPruneInboxCommand extends Command
{
    public const int DEFAULT_RETENTION_DAYS = 7;
    public const int MIN_RETENTION_DAYS = 1;
    public const int DEFAULT_BATCH_SIZE = 1000;

    /**
     * @param list<string> $transportNames Every messenger transport name; the inbox transports are picked at runtime (DSNs may come from env vars)
     */
    public function __construct(
        private readonly ContainerInterface $transportLocator,
        private readonly array $transportNames = [],
    ) {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this
            ->addArgument('transports', InputArgument::IS_ARRAY | InputArgument::OPTIONAL, 'Inbox transport names to prune (default: every inbox transport)')
            ->addOption('retention-days', null, InputOption::VALUE_REQUIRED, \sprintf('Keep processed entries for this many days (minimum %d)', self::MIN_RETENTION_DAYS), (string) self::DEFAULT_RETENTION_DAYS)
            ->addOption('batch-size', null, InputOption::VALUE_REQUIRED, 'Rows deleted per statement', (string) self::DEFAULT_BATCH_SIZE)
            ->addOption('dry-run', null, InputOption::VALUE_NONE, 'Only count the entries that would be deleted')
            ->setHelp(<<<'HELP'
                The <info>%command.name%</info> command deletes the dedup index entries of the inbox
                transports whose message was processed (acked or permanently rejected) more than
                <comment>--retention-days</comment> days ago. Pending entries are never touched.

                While an entry exists, a broker redelivery of its message UUID is dropped; once it is
                pruned, such a redelivery is handled again. Keep the retention far beyond every
                redelivery and retry window — days, not minutes.

                    <info>php %command.full_name%</info>
                    <info>php %command.full_name% --retention-days=30 commands_inbox</info>
                    <info>php %command.full_name% --dry-run</info>
                HELP);
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $io = new SymfonyStyle($input, $output);

        $retentionDays = filter_var($input->getOption('retention-days'), \FILTER_VALIDATE_INT);
        if (!\is_int($retentionDays) || $retentionDays < self::MIN_RETENTION_DAYS) {
            $io->error(\sprintf('The --retention-days option must be an integer of at least %d: the retention must lie far beyond the redelivery and retry windows.', self::MIN_RETENTION_DAYS));

            return Command::INVALID;
        }

        $batchSize = filter_var($input->getOption('batch-size'), \FILTER_VALIDATE_INT);
        if (!\is_int($batchSize) || $batchSize < 1) {
            $io->error('The --batch-size option must be a positive integer.');

            return Command::INVALID;
        }

        $requested = $input->getArgument('transports');
        $requested = \is_array($requested) ? array_values(array_filter($requested, \is_string(...))) : [];
        $transports = $this->resolveInboxTransports($requested, $io);
        if (null === $transports) {
            return Command::FAILURE;
        }
        if ([] === $transports) {
            $io->note('No inbox transport found.');

            return Command::SUCCESS;
        }

        $dryRun = (bool) $input->getOption('dry-run');
        $processedBefore = new \DateTimeImmutable('UTC')->modify(\sprintf('-%d days', $retentionDays));

        $rows = [];
        $total = 0;
        foreach ($transports as $name => $transport) {
            $connection = $transport->getConnection();
            if (!$connection->isDeduplicating()) {
                $rows[] = [$name, '-', 'no deduplication'];
                continue;
            }

            $count = $dryRun
                ? $connection->countProcessed($processedBefore)
                : $connection->pruneProcessed($processedBefore, $batchSize);
            $total += $count;
            $rows[] = [$name, $connection->getIndexTableName(), (string) $count];
        }

        $io->table(['Transport', 'Index table', $dryRun ? 'Would delete' : 'Deleted'], $rows);
        $io->success(\sprintf(
            '%s %d processed inbox %s processed before %s UTC (retention: %d %s).',
            $dryRun ? 'Would delete' : 'Deleted',
            $total,
            1 === $total ? 'entry' : 'entries',
            $processedBefore->format('Y-m-d H:i:s'),
            $retentionDays,
            1 === $retentionDays ? 'day' : 'days',
        ));

        return Command::SUCCESS;
    }

    /**
     * @param list<string> $requested
     *
     * @return array<string, InboxTransport>|null Null when a requested transport is unknown or not an inbox
     */
    private function resolveInboxTransports(array $requested, SymfonyStyle $io): ?array
    {
        if ([] !== $requested) {
            $transports = [];
            foreach ($requested as $name) {
                $transport = $this->transportLocator->has($name) ? $this->transportLocator->get($name) : null;
                if (!$transport instanceof InboxTransport) {
                    $io->error(\sprintf('The transport "%s" %s.', $name, null === $transport ? 'does not exist' : 'is not an inbox transport'));

                    return null;
                }
                $transports[$name] = $transport;
            }

            return $transports;
        }

        $transports = [];
        foreach ($this->transportNames as $name) {
            $transport = $this->transportLocator->has($name) ? $this->transportLocator->get($name) : null;
            if ($transport instanceof InboxTransport) {
                $transports[$name] = $transport;
            }
        }

        return $transports;
    }
}
