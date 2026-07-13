<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Messenger\Console;

use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\DependencyInjection\ParameterBag\ContainerBagInterface;
use Webmozart\Assert\Assert;

#[AsCommand(name: 'messenger:supervisor-config', description: 'Generate supervisor config for the messenger workflow workers')]
class MessengerSupervisorConfigCommand extends Command
{
    public function __construct(
        protected array $workersConfig,
        protected array $workersDefaultConfig,
        protected ContainerBagInterface $params,
    ) {
        parent::__construct();
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $supervisorConfig = $this->generateSupervisorConfig($this->workersConfig);
        $output->writeln($supervisorConfig);

        return Command::SUCCESS;
    }

    private function generateSupervisorConfig(array $config): string
    {
        $groups = [];
        foreach ($config as $item) {
            $group = $this->formatName($item['group']);
            Assert::notEmpty($group);
            $name = $this->formatName($item['name']);
            Assert::notEmpty($name);
            if (isset($groups[$group][$name])) {
                throw new \LogicException(\sprintf('The program "%s" is already defined for group "%s"', $name, $group));
            }
            $groups[$group][$name] = $item;
        }
        $sc = [];
        foreach ($groups as $group => $programs) {
            $sc = array_merge($sc, array_values($this->buildSupervisorGroupConfig($group, array_keys($programs))));
            foreach ($programs as $name => $item) {
                $program = $this->buildSupervisorProgramConfig($name, $item);
                $sc = array_merge($sc, array_values($program));
            }
        }

        return implode(\PHP_EOL, $sc);
    }

    private function buildSupervisorGroupConfig(string $name, array $programNames): array
    {
        return [
            \sprintf('[group:%s]', $name),
            \sprintf('programs=%s', implode(',', $programNames)),
            '',
        ];
    }

    private function buildSupervisorProgramConfig(string $name, array $config): array
    {
        $config = array_replace_recursive($this->workersDefaultConfig, $config);
        $program = [
            \sprintf('[program:%s]', $name),
            'command' => \sprintf('command=%s', $this->buildMessengerConsumeCommand($config)),
            'environment' => '',
            'directory' => \sprintf('directory=%s', $this->params->get('kernel.project_dir')),
            'numprocs' => \sprintf('numprocs=%s', $config['instances']),
            'numprocs_start' => \sprintf('numprocs_start=%s', 1),
            'process_name' => $config['instances'] > 1 ? 'process_name=%(program_name)s-%(process_num)02d' : 'process_name=%(program_name)s',
        ];
        $override = [];
        foreach ($config['supervisor'] ?? [] as $key => $value) {
            $override[$key] = \sprintf('%s=%s', $key, $value);
        }
        $override['environment'] = implode(',', array_filter([
            $override['environment'] ?? '',
            \sprintf('MSG_BROKER_CONN_NAME="%s"', $name),
        ]));
        if ('' === $override['environment']) {
            unset($override['environment']);
        } else {
            $override['environment'] = 'environment='.$override['environment'];
        }
        $program = array_replace($program, $override);
        $program[] = '';

        return $program;
    }

    private function buildMessengerConsumeCommand(array $config): string
    {
        $source = $config['source'] ?? null;
        Assert::stringNotEmpty($source);
        $target = $config['target'] ?? null;
        Assert::stringNotEmpty($target);
        $queue = $config['queue'] ?? null;
        $extraOptions = $config['cmd_extra_options'] ?? [];
        $options = [
            'limit' => '--limit=%s',
            'failure_limit' => '--failure-limit=%s',
            'memory_limit' => '--memory-limit=%s',
            'time_limit' => '--time-limit=%s',
            'fetch_size' => '--fetch-size=%s',
            'sleep' => '--sleep=%s',
            'verbose' => '%s',
        ];
        $options = array_intersect_key($options, $extraOptions);
        foreach ($options as $option => $format) {
            $options[$option] = \sprintf($format, $extraOptions[$option]);
        }
        /** @phpstan-ignore notIdentical.alwaysTrue */
        $options = array_filter($options, fn ($v) => '' !== $v && null !== $v);
        $cmd = array_filter([
            'bin/console messenger:consume',
            'target' => '--bus='.$target,
            'queue' => $queue ? '--queues='.$queue : '',
            'options' => implode(' ', $options),
            'source' => $source,
        /** @phpstan-ignore notIdentical.alwaysTrue */
        ], fn ($v) => '' !== $v && null !== $v);

        return implode(' ', $cmd);
    }

    private function formatName(string $name): string
    {
        $result = preg_replace([
            '/\s+/',
            '/[_ ]+/',
            '/^[^a-zA-Z]+|[^a-zA-Z\-]+/',
        ], [
            ' ',
            '-',
            '',
        ], $name);
        $result = preg_replace_callback('/[A-Z]/', fn ($matches) => '_'.$matches[0], $result);
        $result = mb_ltrim($result, '_');

        return mb_strtolower($result);
    }
}
