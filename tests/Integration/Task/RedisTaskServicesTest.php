<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Integration\Task;

use Kraz\MessengerWorkflow\Application\Exception\TaskFailedException;
use Kraz\MessengerWorkflow\Application\Task\Exception\TaskNotFoundException;
use Kraz\MessengerWorkflow\Application\Task\TaskStatus;
use Kraz\MessengerWorkflow\Infrastructure\Task\RedisResultStorage;
use Kraz\MessengerWorkflow\Infrastructure\Task\RedisTaskOwnershipRegistry;
use Kraz\MessengerWorkflow\Infrastructure\Task\RedisTaskResultProvider;
use Kraz\MessengerWorkflow\Infrastructure\Task\RedisTaskStatusProvider;
use Kraz\MessengerWorkflow\Tests\Support\Infra;
use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\TestCase;

/**
 * Spec: the Redis task services against a real Redis — status truth
 * table, TTL interplay (ownership outlives results), non-destructive reads, and the
 * non-blocking result provider.
 */
#[Group('redis')]
final class RedisTaskServicesTest extends TestCase
{
    private const string NAMESPACE = 'p10';

    private \Redis $redis;
    private RedisResultStorage $resultStorage;
    private RedisTaskOwnershipRegistry $ownership;
    private RedisTaskStatusProvider $statusProvider;
    private RedisTaskResultProvider $resultProvider;

    protected function setUp(): void
    {
        $this->redis = Infra::requireRedis();
        foreach (array_merge(
            (array) $this->redis->keys('rs:'.self::NAMESPACE.':*'),
            (array) $this->redis->keys('task-meta:p10-*'),
        ) as $key) {
            if (\is_string($key)) {
                $this->redis->del($key);
            }
        }

        $this->resultStorage = new RedisResultStorage($this->redis, expireInputAfter: 3 * 60 * 60, namespace: self::NAMESPACE);
        $this->ownership = new RedisTaskOwnershipRegistry($this->redis, ownershipTtl: 4 * 60 * 60);
        $this->statusProvider = new RedisTaskStatusProvider($this->redis, $this->ownership, debug: true, resultNamespace: self::NAMESPACE);
        $this->resultProvider = new RedisTaskResultProvider($this->redis, self::NAMESPACE);
    }

    public function testUnknownTaskIsNotFound(): void
    {
        $this->expectException(TaskNotFoundException::class);

        $this->statusProvider->getStatus('p10-unknown');
    }

    public function testOwnershipOnlyMeansPending(): void
    {
        $this->ownership->record('p10-pending', 'user-1', 'App\SomeCommand');

        $status = $this->statusProvider->getStatus('p10-pending');

        self::assertSame(TaskStatus::Pending, $status->status);
        self::assertNull($status->error);
        self::assertNotNull($status->startedAt);

        $owner = $this->ownership->find('p10-pending');
        self::assertSame('user-1', $owner?->userIdentifier);
        self::assertSame('App\SomeCommand', $owner->messageType);
    }

    public function testAStoredResultMeansCompleted(): void
    {
        $this->resultStorage->write('p10-done', ['value' => 42]);

        $status = $this->statusProvider->getStatus('p10-done');

        self::assertSame(TaskStatus::Completed, $status->status);
        self::assertNull($status->error);
        self::assertNull($status->startedAt, 'No ownership record — a system task');
    }

    public function testAStoredErrorMeansFailedWithMaskedDetails(): void
    {
        $this->resultStorage->writeError('p10-failed', 'went wrong', 7, \RuntimeException::class, 'trace...');

        $status = $this->statusProvider->getStatus('p10-failed');

        self::assertSame(TaskStatus::Failed, $status->status);
        self::assertSame('went wrong', $status->error?->message);
        self::assertSame(7, $status->error->code);
        self::assertSame(\RuntimeException::class, $status->error->class, 'debug=true exposes the class');

        $masked = new RedisTaskStatusProvider($this->redis, $this->ownership, debug: false, resultNamespace: self::NAMESPACE);
        self::assertNull($masked->getStatus('p10-failed')->error?->class, 'debug=false masks the class');
    }

    public function testAnUndecodableResultMeansUnknownNotCompleted(): void
    {
        $this->redis->set('rs:'.self::NAMESPACE.':p10-corrupt', "\x00{not-json", ['ex' => 60]);

        $status = $this->statusProvider->getStatus('p10-corrupt');

        self::assertSame(TaskStatus::Unknown, $status->status, 'A corrupt result proves the task ended, but must not be reported as success');
        self::assertNull($status->error);
    }

    public function testStatusReadsAreNonDestructive(): void
    {
        $this->resultStorage->write('p10-ttl', 'v');
        $ttlBefore = $this->redis->ttl('rs:'.self::NAMESPACE.':p10-ttl');

        $this->statusProvider->getStatus('p10-ttl');
        $this->statusProvider->getStatus('p10-ttl');

        $ttlAfter = $this->redis->ttl('rs:'.self::NAMESPACE.':p10-ttl');
        self::assertIsInt($ttlBefore);
        self::assertIsInt($ttlAfter);
        self::assertGreaterThan($ttlBefore - 5, $ttlAfter, 'Peeking must not shorten the result TTL');
    }

    public function testOwnershipTtlOutlivesTheResultTtl(): void
    {
        $this->ownership->record('p10-ttl2', 'user-1');
        $this->resultStorage->write('p10-ttl2', 'v');

        $ownershipTtl = $this->redis->ttl('task-meta:p10-ttl2');
        $resultTtl = $this->redis->ttl('rs:'.self::NAMESPACE.':p10-ttl2');

        self::assertIsInt($ownershipTtl);
        self::assertIsInt($resultTtl);
        self::assertGreaterThan($resultTtl, $ownershipTtl);
    }

    public function testResultProviderReturnsTheValueWithoutBlocking(): void
    {
        $this->resultStorage->write('p10-value', ['answer' => 42]);

        self::assertSame(['answer' => 42], $this->resultProvider->getResult('p10-value'));
    }

    public function testResultProviderThrowsTaskFailedForErrorOutcomes(): void
    {
        $this->resultStorage->writeError('p10-err', 'boom', 3, \DomainException::class);

        try {
            $this->resultProvider->getResult('p10-err');
            self::fail('An error outcome must throw');
        } catch (TaskFailedException $exception) {
            self::assertSame('boom', $exception->getMessage());
            self::assertSame(\DomainException::class, $exception->getTaskClass());
        }
    }

    public function testResultProviderThrowsNotFoundForMissingResults(): void
    {
        $this->expectException(TaskNotFoundException::class);

        $this->resultProvider->getResult('p10-missing');
    }

    public function testBestEffortOwnershipNeverThrows(): void
    {
        $broken = new \Redis();
        $registry = new RedisTaskOwnershipRegistry($broken);

        $registry->record('p10-x', 'user');
        self::assertNull($registry->find('p10-x'));
    }
}
