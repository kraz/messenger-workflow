<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Infrastructure\Console;

use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\DependencyInjection\ParameterBag\ContainerBagInterface;

/**
 * Generates the supervisord configuration for the workflow workers (ported from the
 * original package — same output shape: [group:x]/[program:y] blocks, numprocs,
 * MSG_BROKER_CONN_NAME environment, merged supervisor overrides).
 *
 * By default the configuration is printed to stdout (BC); --output-dir writes one
 * "<group>.conf" file per worker group instead (D11).
 */
#[AsCommand(name: 'messenger:supervisor-config', description: 'Generate supervisor config for the messenger workflow workers')]
class MessengerSupervisorConfigCommand extends Command
{
    /**
     * @param list<array<array-key, mixed>> $workersConfig
     * @param array<array-key, mixed>       $workersDefaultConfig
     */
    public function __construct(
        protected array $workersConfig,
        protected array $workersDefaultConfig,
        protected ContainerBagInterface $params,
    ) {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this->addOption('output-dir', null, InputOption::VALUE_REQUIRED, 'Write one "<group>.conf" file per worker group into this directory instead of printing to stdout');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $groups = $this->groupPrograms($this->workersConfig);

        $outputDir = $input->getOption('output-dir');
        if (\is_string($outputDir) && '' !== $outputDir) {
            if (!is_dir($outputDir) && !mkdir($outputDir, 0o777, true) && !is_dir($outputDir)) {
                $output->writeln(\sprintf('<error>Cannot create output directory "%s".</error>', $outputDir));

                return Command::FAILURE;
            }
            foreach ($groups as $group => $programs) {
                $file = rtrim($outputDir, '/').'/'.$group.'.conf';
                file_put_contents($file, $this->renderGroup($group, $programs).\PHP_EOL);
                $output->writeln(\sprintf('Wrote <info>%s</info>', $file));
            }

            return Command::SUCCESS;
        }

        $sections = [];
        foreach ($groups as $group => $programs) {
            $sections[] = $this->renderGroup($group, $programs);
        }
        $output->writeln(implode(\PHP_EOL, $sections));

        return Command::SUCCESS;
    }

    /**
     * @param list<array<array-key, mixed>> $config
     *
     * @return array<string, array<string, array<array-key, mixed>>>
     */
    private function groupPrograms(array $config): array
    {
        $groups = [];
        foreach ($config as $item) {
            $group = $this->formatName($this->stringOption($item, 'group'));
            if ('' === $group) {
                throw new \LogicException('A worker configuration is missing its "group".');
            }
            $name = $this->formatName($this->stringOption($item, 'name'));
            if ('' === $name) {
                throw new \LogicException(\sprintf('A worker configuration of group "%s" is missing its "name".', $group));
            }
            if (isset($groups[$group][$name])) {
                throw new \LogicException(\sprintf('The program "%s" is already defined for group "%s"', $name, $group));
            }
            $groups[$group][$name] = $item;
        }

        return $groups;
    }

    /**
     * @param array<string, array<array-key, mixed>> $programs
     */
    private function renderGroup(string $group, array $programs): string
    {
        $lines = $this->buildSupervisorGroupConfig($group, array_keys($programs));
        foreach ($programs as $name => $item) {
            $lines = array_merge($lines, array_values($this->buildSupervisorProgramConfig($name, $item)));
        }

        return implode(\PHP_EOL, $lines);
    }

    /**
     * @param list<string> $programNames
     *
     * @return list<string>
     */
    private function buildSupervisorGroupConfig(string $name, array $programNames): array
    {
        return [
            \sprintf('[group:%s]', $name),
            \sprintf('programs=%s', implode(',', $programNames)),
            '',
        ];
    }

    /**
     * @param array<array-key, mixed> $config
     *
     * @return array<array-key, string>
     */
    private function buildSupervisorProgramConfig(string $name, array $config): array
    {
        $config = array_replace_recursive($this->workersDefaultConfig, $config);
        $instances = is_numeric($config['instances'] ?? null) ? (int) $config['instances'] : 1;
        $projectDir = $this->params->get('kernel.project_dir');
        $program = [
            \sprintf('[program:%s]', $name),
            'command' => \sprintf('command=%s', $this->buildMessengerConsumeCommand($config)),
            'environment' => '',
            'directory' => \sprintf('directory=%s', \is_scalar($projectDir) ? (string) $projectDir : ''),
            'numprocs' => \sprintf('numprocs=%s', $instances),
            'numprocs_start' => \sprintf('numprocs_start=%s', 1),
            'process_name' => $instances > 1 ? 'process_name=%(program_name)s-%(process_num)02d' : 'process_name=%(program_name)s',
        ];

        $override = [];
        $environmentOverride = '';
        $supervisorOverrides = \is_array($config['supervisor'] ?? null) ? $config['supervisor'] : [];
        foreach ($supervisorOverrides as $key => $value) {
            if ('environment' === $key) {
                $environmentOverride = \is_scalar($value) ? (string) $value : '';
                continue;
            }
            $override[$key] = \sprintf('%s=%s', $key, \is_scalar($value) ? (string) $value : '');
        }
        $override['environment'] = 'environment='.implode(',', array_filter([
            $environmentOverride,
            \sprintf('MSG_BROKER_CONN_NAME="%s"', $name),
        ], static fn (string $v): bool => '' !== $v));

        $program = array_replace($program, $override);
        $program[] = '';

        return $program;
    }

    /**
     * @param array<array-key, mixed> $config
     */
    private function buildMessengerConsumeCommand(array $config): string
    {
        $source = $this->stringOption($config, 'source');
        if ('' === $source) {
            throw new \LogicException('A worker configuration is missing its "source" transport.');
        }
        $target = $this->stringOption($config, 'target');
        if ('' === $target) {
            throw new \LogicException(\sprintf('The worker consuming "%s" is missing its "target" bus.', $source));
        }
        $queue = $this->stringOption($config, 'queue');

        $extraOptions = \is_array($config['cmd_extra_options'] ?? null) ? $config['cmd_extra_options'] : [];
        $optionFormats = [
            'limit' => '--limit=%s',
            'failure_limit' => '--failure-limit=%s',
            'memory_limit' => '--memory-limit=%s',
            'time_limit' => '--time-limit=%s',
            'fetch_size' => '--fetch-size=%s',
            'sleep' => '--sleep=%s',
            'verbose' => '%s',
        ];
        $options = [];
        foreach ($optionFormats as $option => $format) {
            if (\array_key_exists($option, $extraOptions) && \is_scalar($extraOptions[$option])) {
                $options[$option] = \sprintf($format, (string) $extraOptions[$option]);
            }
        }
        $options = array_filter($options, static fn (string $v): bool => '' !== $v);

        $cmd = array_filter([
            'bin/console messenger:consume',
            'target' => '--bus='.$target,
            'queue' => '' !== $queue ? '--queues='.$queue : '',
            'options' => implode(' ', $options),
            'source' => $source,
        ], static fn (string $v): bool => '' !== $v);

        return implode(' ', $cmd);
    }

    /**
     * @param array<array-key, mixed> $config
     */
    private function stringOption(array $config, string $key): string
    {
        return \is_scalar($config[$key] ?? null) ? (string) $config[$key] : '';
    }

    private function formatName(string $name): string
    {
        $result = (string) preg_replace([
            '/\s+/',
            '/[_ ]+/',
            '/^[^a-zA-Z]+|[^a-zA-Z\-]+/',
        ], [
            ' ',
            '-',
            '',
        ], $name);
        $result = (string) preg_replace_callback('/[A-Z]/', static fn (array $matches): string => '_'.$matches[0], $result);
        $result = mb_ltrim($result, '_');

        return mb_strtolower($result);
    }
}
