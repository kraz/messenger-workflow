<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Integration\Transport;

use Kraz\MessengerWorkflow\Application\Exception\TaskFailedException;
use Kraz\MessengerWorkflow\Messenger\Transport\Exception\ResultStorageWaitTimeoutException;
use Kraz\MessengerWorkflow\Messenger\Transport\RedisResultStorage;
use Kraz\MessengerWorkflow\Tests\Fixture\Message\TestQuery;
use Kraz\MessengerWorkflow\Tests\Fixture\RedisClientFactory;
use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\TestCase;

/**
 * Spec: Task results retrievable by UUID; task status/result stored in Redis.
 * Runs against the live Redis from README (localhost:6379, auth "xxx"), using db 15.
 */
#[Group('redis')]
final class RedisResultStorageTest extends TestCase
{
    private \Redis $redis;
    private string $namespace;
    private RedisResultStorage $storage;

    protected function setUp(): void
    {
        try {
            $this->redis = RedisClientFactory::create();
        } catch (\RedisException $e) {
            self::markTestSkipped('Redis is not reachable: '.$e->getMessage());
        }
        $this->namespace = 'mwftest-'.bin2hex(random_bytes(4));
        $this->storage = new RedisResultStorage(
            $this->redis,
            expireInputAfter: 60,
            expireOutputAfter: 5,
            namespace: $this->namespace,
        );
    }

    protected function tearDown(): void
    {
        if (isset($this->redis)) {
            $keys = $this->redis->keys('rs:'.$this->namespace.':*');
            if ($keys) {
                $this->redis->del($keys);
            }
        }
    }

    public function testScalarAndArrayResultsRoundTrip(): void
    {
        $this->storage->write('id-scalar', 42);
        self::assertSame(42, $this->storage->await('id-scalar', 1));

        $this->storage->write('id-array', ['a' => 1, 'b' => ['c' => true]]);
        self::assertSame(['a' => 1, 'b' => ['c' => true]], $this->storage->await('id-array', 1));

        $this->storage->write('id-null', null);
        self::assertNull($this->storage->await('id-null', 1));
    }

    public function testObjectResultRoundTrip(): void
    {
        $this->storage->write('id-object', new TestQuery('round-trip'));

        $result = $this->storage->await('id-object', 1);

        self::assertInstanceOf(TestQuery::class, $result);
        self::assertSame('round-trip', $result->subject);
    }

    public function testKeysAreNamespaced(): void
    {
        $this->storage->write('id-key', 'v');

        self::assertNotFalse($this->redis->get('rs:'.$this->namespace.':id-key'));
    }

    public function testWriteSetsInputTtlAndAwaitShortensIt(): void
    {
        $this->storage->write('id-ttl', 'v');
        $key = 'rs:'.$this->namespace.':id-ttl';

        $ttlAfterWrite = $this->redis->ttl($key);
        self::assertGreaterThan(5, $ttlAfterWrite);
        self::assertLessThanOrEqual(60, $ttlAfterWrite);

        $this->storage->await('id-ttl', 1);

        self::assertLessThanOrEqual(5, $this->redis->ttl($key));
    }

    public function testWriteErrorSurfacesAsTaskFailedException(): void
    {
        $this->storage->writeError('id-err', 'remote handler failed', 7, \DomainException::class, '#0 remote-trace');

        try {
            $this->storage->await('id-err', 1);
            self::fail('Expected TaskFailedException');
        } catch (TaskFailedException $e) {
            self::assertSame('remote handler failed', $e->getMessage());
            self::assertSame(7, $e->getCode());
            self::assertSame(\DomainException::class, $e->getTaskClass());
            self::assertSame('#0 remote-trace', $e->getTaskTrace());
        }
    }

    public function testAwaitTimesOutWhenNoResultArrives(): void
    {
        $this->expectException(ResultStorageWaitTimeoutException::class);

        $this->storage->await('id-never', 1);
    }

    /**
     * Regression test: await() used a second-resolution deadline,
     * so a timeout of N seconds could expire after only N-1+epsilon seconds.
     */
    public function testAwaitWaitsAtLeastTheRequestedTimeout(): void
    {
        $start = microtime(true);

        try {
            $this->storage->await('id-never-full', 1);
            self::fail('Expected ResultStorageWaitTimeoutException');
        } catch (ResultStorageWaitTimeoutException) {
            self::assertGreaterThanOrEqual(1.0, microtime(true) - $start);
        }
    }

    public function testAwaitPicksUpResultWrittenWhilePolling(): void
    {
        // Simulates the remote worker completing while the caller is blocked in await()
        $namespace = $this->namespace;
        $pid = pcntl_fork();
        if (0 === $pid) {
            // child: write the result after a short delay on a fresh connection
            usleep(300_000);
            $redis = RedisClientFactory::create();
            $storage = new RedisResultStorage($redis, 60, 5, $namespace);
            $storage->write('id-late', 'late-result');
            // Die without running PHP shutdown/destructors: the child inherits every socket the
            // parent has open (AMQP, PG, ...), and destructing those copies would send protocol
            // close frames on the shared sockets, corrupting the parent's connections.
            pcntl_exec('/bin/true');
            exit(0); // unreachable unless /bin/true is missing
        }

        try {
            self::assertSame('late-result', $this->storage->await('id-late', 5));
        } finally {
            pcntl_waitpid($pid, $status);
        }
    }
}
