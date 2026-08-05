<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Integration\Flow;

use Contracts\Demo\Query\AmbiguousQuery;
use Contracts\Demo\Query\GetSomethingQuery;
use Contracts\Demo\Query\UnhandledQuery;
use Kraz\MessengerWorkflow\Application\Exception\TaskFailedException;
use Kraz\MessengerWorkflow\Application\Exception\TaskTimeOutException;
use Kraz\MessengerWorkflow\Application\Messenger\QueryBusInterface;
use Kraz\MessengerWorkflow\Tests\Fixture\Handler\GetSomethingQueryHandler;
use Kraz\MessengerWorkflow\Tests\Support\Infra;
use Kraz\MessengerWorkflow\Tests\Support\WorkflowKernelTestCase;
use Kraz\MessengerWorkflow\Tests\TestKernel\RedisResultStorageKernel;
use Jwage\PhpAmqpLibMessengerBundle\Transport\AmqpTransport;
use PHPUnit\Framework\Attributes\Group;
use Symfony\Bundle\FrameworkBundle\Console\Application;
use Symfony\Component\Console\Input\ArrayInput;
use Symfony\Component\Console\Output\BufferedOutput;
use Symfony\Component\EventDispatcher\EventDispatcherInterface;
use Symfony\Component\Messenger\EventListener\StopWorkerOnMessageLimitListener;
use Symfony\Component\Messenger\EventListener\StopWorkerOnTimeLimitListener;
use Symfony\Component\Messenger\MessageBusInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;
use Symfony\Component\Messenger\Worker;

/**
 * Spec: query flow end-to-end — ask()/askAsync() → RabbitMQ (direct exchange,
 * no outbox/inbox segments) → handler worker → result storage → await. Aggressive
 * transient retries through the AMQP delay exchange; NO failure transport: permanent
 * failures are reported to the asker through the result storage and the message is
 * dropped.
 *
 * ask() itself is askAsync()+await() (unit-tested); the single-process E2E uses the
 * split form so the handler worker can run between dispatch and await.
 */
#[Group('rabbitmq')]
#[Group('redis')]
final class QueryFlowTest extends WorkflowKernelTestCase
{
    private const string BROKER = 'queries';

    private \Redis $redis;

    protected static function getKernelClass(): string
    {
        return RedisResultStorageKernel::class;
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->redis = Infra::requireRedis();
        $amqp = Infra::requireAmqp();

        self::bootKernel();
        $kernel = self::$kernel;
        self::assertNotNull($kernel);
        $application = new Application($kernel);
        $application->setAutoExit(false);
        $output = new BufferedOutput();
        self::assertSame(0, $application->run(new ArrayInput(['command' => 'messenger:setup-transports', '--no-interaction' => true]), $output), $output->fetch());

        $channel = $amqp->channel();
        $channel->queue_purge('app_queries');
        $channel->close();
        $amqp->close();

        $keys = $this->redis->keys('rs:fk:*');
        foreach (\is_array($keys) ? $keys : [] as $key) {
            $this->redis->del($key);
        }

        GetSomethingQueryHandler::reset();
    }

    private function queryBus(): QueryBusInterface
    {
        $bus = self::getContainer()->get(QueryBusInterface::class);
        self::assertInstanceOf(QueryBusInterface::class, $bus);

        return $bus;
    }

    private function runQueryWorker(int $messageLimit, int $timeLimit = 10): void
    {
        $container = self::getContainer();
        $transport = $container->get('messenger.transport.'.self::BROKER);
        self::assertInstanceOf(TransportInterface::class, $transport);
        $bus = $container->get('query.bus');
        self::assertInstanceOf(MessageBusInterface::class, $bus);
        $dispatcher = $container->get('event_dispatcher');
        self::assertInstanceOf(EventDispatcherInterface::class, $dispatcher);

        $stopOnLimit = new StopWorkerOnMessageLimitListener($messageLimit);
        $stopOnTime = new StopWorkerOnTimeLimitListener($timeLimit);
        $dispatcher->addSubscriber($stopOnLimit);
        $dispatcher->addSubscriber($stopOnTime);
        try {
            new Worker([self::BROKER => $transport], $bus, $dispatcher)->run(['sleep' => 50000]);
        } finally {
            $dispatcher->removeSubscriber($stopOnLimit);
            $dispatcher->removeSubscriber($stopOnTime);
            // A dangling consumer (open prefetch window) would steal messages
            // published by the NEXT test in this process.
            if ($transport instanceof AmqpTransport) {
                $transport->getConnection()->close();
            }
        }
    }

    public function testQueryRoundTrip(): void
    {
        $taskId = $this->queryBus()->askAsync(new GetSomethingQuery('round-trip'));

        $this->runQueryWorker(1);

        self::assertSame(['answer' => 'value:round-trip', 'attempt' => 1], $this->queryBus()->await($taskId, 5));
        self::assertSame(1, GetSomethingQueryHandler::attempts('round-trip'));
    }

    public function testConcurrentAskersGetTheirOwnResults(): void
    {
        $bus = $this->queryBus();
        $taskIds = [];
        foreach (['a', 'b', 'c'] as $payload) {
            $taskIds[$payload] = $bus->askAsync(new GetSomethingQuery($payload));
        }

        $this->runQueryWorker(3);

        // Await in reverse order — results are keyed by task, not by arrival.
        foreach (array_reverse($taskIds, true) as $payload => $taskId) {
            self::assertSame(['answer' => 'value:'.$payload, 'attempt' => 1], $bus->await($taskId, 5));
        }
    }

    public function testTransientFailuresAreRetriedThroughTheDelayExchange(): void
    {
        $taskId = $this->queryBus()->askAsync(new GetSomethingQuery('transient-once'));

        $this->runQueryWorker(2, timeLimit: 15);

        self::assertSame(['answer' => 'transient-recovered', 'attempt' => 2], $this->queryBus()->await($taskId, 5));
        self::assertSame(2, GetSomethingQueryHandler::attempts('transient-once'));
    }

    public function testPermanentHandlerFailureIsReportedToTheAskerAndTheMessageIsDropped(): void
    {
        $taskId = $this->queryBus()->askAsync(new GetSomethingQuery('fail'));

        $this->runQueryWorker(1);

        try {
            $this->queryBus()->await($taskId, 5);
            self::fail('The asker must receive the failure');
        } catch (TaskFailedException $exception) {
            self::assertSame('query failed permanently', $exception->getMessage());
            self::assertSame(23, $exception->getCode());
            self::assertSame(\RuntimeException::class, $exception->getTaskClass());
        }

        self::assertSame(1, GetSomethingQueryHandler::attempts('fail'), 'Non-transient failures are not retried');
    }

    public function testZeroHandlersFailsTheAsker(): void
    {
        $taskId = $this->queryBus()->askAsync(new UnhandledQuery('nobody'));

        $this->runQueryWorker(1);

        try {
            $this->queryBus()->await($taskId, 5);
            self::fail('The asker must receive the failure');
        } catch (TaskFailedException $exception) {
            self::assertStringContainsString('handled zero times', $exception->getMessage());
        }
    }

    public function testMultipleHandlersFailTheAsker(): void
    {
        $taskId = $this->queryBus()->askAsync(new AmbiguousQuery('both'));

        $this->runQueryWorker(1);

        try {
            $this->queryBus()->await($taskId, 5);
            self::fail('The asker must receive the failure');
        } catch (TaskFailedException $exception) {
            self::assertStringContainsString('handled multiple times', $exception->getMessage());
        }
    }

    public function testAwaitTimesOutWhenNoResultArrives(): void
    {
        $this->expectException(TaskTimeOutException::class);

        $this->queryBus()->await('01890000-0000-7000-8000-000000000000', 1);
    }
}
