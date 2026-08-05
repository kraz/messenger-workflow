<?php

declare(strict_types=1);

namespace Kraz\MessengerWorkflow\Tests\Integration\Task;

use Kraz\MessengerWorkflow\Application\Exception\TaskFailedException;
use Kraz\MessengerWorkflow\Application\Task\Exception\ResultStorageWaitTimeoutException;
use Kraz\MessengerWorkflow\Infrastructure\Task\RedisResultStorage;
use Kraz\MessengerWorkflow\Tests\Fixture\Message\TestQuery;
use Kraz\MessengerWorkflow\Tests\Fixture\RedisClientFactory;
use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\Attributes\RequiresPhpExtension;
use PHPUnit\Framework\TestCase;

/**
 * Spec: task results retrievable by UUID; awaiting must never shorten the result TTL
 * unless explicitly configured. Runs against the local development Redis, database 15.
 */
#[Group('redis')]
#[RequiresPhpExtension('redis')]
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
            namespace: $this->namespace,
        );
    }

    protected function tearDown(): void
    {
        if (isset($this->redis)) {
            $keys = $this->redis->keys('rs:'.$this->namespace.':*');
            if (\is_array($keys) && [] !== $keys) {
                $this->redis->del($keys);
            }
            $this->redis->close();
        }
    }

    public function testScalarArrayAndNullResultsRoundTrip(): void
    {
        $this->storage->write('id-scalar', 42);
        self::assertSame(42, $this->storage->await('id-scalar', 1));

        $this->storage->write('id-array', ['a' => 1, 'b' => ['c' => true]]);
        self::assertSame(['a' => 1, 'b' => ['c' => true]], $this->storage->await('id-array', 1));

        $this->storage->write('id-null', null);
        self::assertNull($this->storage->await('id-null', 1));
    }

    public function testObjectResultsRoundTrip(): void
    {
        $this->storage->write('id-object', new TestQuery('payload-x'));

        $result = $this->storage->await('id-object', 1);

        self::assertInstanceOf(TestQuery::class, $result);
        self::assertSame('payload-x', $result->payload);
    }

    public function testErrorPayloadThrowsTaskFailedException(): void
    {
        $this->storage->writeError('id-err', 'Handler exploded', 13, 'App\SomeCommand', '#0 remote-frame');

        try {
            $this->storage->await('id-err', 1);
            self::fail('Expected TaskFailedException');
        } catch (TaskFailedException $exception) {
            self::assertSame('Handler exploded', $exception->getMessage());
            self::assertSame(13, $exception->getCode());
            self::assertSame('App\SomeCommand', $exception->getTaskClass());
            self::assertSame('#0 remote-frame', $exception->getTaskTrace());
        }
    }

    public function testAwaitTimesOutAfterTheConfiguredTimeout(): void
    {
        $start = microtime(true);

        try {
            $this->storage->await('never-written', 1);
            self::fail('Expected ResultStorageWaitTimeoutException');
        } catch (ResultStorageWaitTimeoutException) {
            $elapsed = microtime(true) - $start;
            self::assertGreaterThanOrEqual(0.9, $elapsed);
            self::assertLessThan(3.0, $elapsed);
        }
    }

    public function testAwaitDoesNotShortenTheResultTtlByDefault(): void
    {
        $this->storage->write('id-ttl', 'value');

        self::assertSame('value', $this->storage->await('id-ttl', 1));
        self::assertSame('value', $this->storage->await('id-ttl', 1), 'Result stays awaitable');

        $ttl = $this->redis->ttl('rs:'.$this->namespace.':id-ttl');
        self::assertIsInt($ttl);
        self::assertGreaterThan(50, $ttl, 'TTL must stay close to expire_input_after (60s)');
    }

    public function testExpireAfterAwaitShortensTheTtlWhenExplicitlyConfigured(): void
    {
        $storage = new RedisResultStorage(
            $this->redis,
            expireInputAfter: 60,
            expireAfterAwait: 5,
            namespace: $this->namespace,
        );

        $storage->write('id-ttl2', 'value');
        self::assertSame('value', $storage->await('id-ttl2', 1));

        $ttl = $this->redis->ttl('rs:'.$this->namespace.':id-ttl2');
        self::assertIsInt($ttl);
        self::assertLessThanOrEqual(5, $ttl);
        self::assertGreaterThan(0, $ttl);
    }

    public function testAwaitBlocksUntilAConcurrentProcessWritesTheResult(): void
    {
        $key = 'rs:'.$this->namespace.':id-concurrent';
        // Same encoding path as RedisResultStorage::write().
        $payload = json_encode(
            new \Symfony\Component\VarExporter\DeepCloner(new \Kraz\MessengerWorkflow\Application\Task\ResultStoragePayload('from-child'))->toArray(),
            \JSON_THROW_ON_ERROR | \JSON_UNESCAPED_SLASHES | \JSON_UNESCAPED_UNICODE,
        );

        $script = <<<'PHP'
            usleep(300000);
            $redis = new Redis();
            $redis->connect($argv[1], (int) $argv[2], 2.0);
            if ('' !== $argv[3]) { $redis->auth($argv[3]); }
            $redis->select(15);
            $redis->setex($argv[4], 60, $argv[5]);
            PHP;

        $host = \is_string($_ENV['MWF_TEST_REDIS_HOST'] ?? null) ? $_ENV['MWF_TEST_REDIS_HOST'] : '127.0.0.1';
        $port = is_numeric($_ENV['MWF_TEST_REDIS_PORT'] ?? null) ? (string) $_ENV['MWF_TEST_REDIS_PORT'] : '6379';
        $auth = \is_string($_ENV['MWF_TEST_REDIS_AUTH'] ?? null) ? $_ENV['MWF_TEST_REDIS_AUTH'] : '';

        $process = proc_open(
            [\PHP_BINARY, '-r', $script, $host, $port, $auth, $key, $payload],
            [2 => ['pipe', 'w']],
            $pipes,
        );
        self::assertIsResource($process);

        try {
            $start = microtime(true);
            $result = $this->storage->await('id-concurrent', 5);

            self::assertSame('from-child', $result);
            self::assertGreaterThanOrEqual(0.25, microtime(true) - $start, 'await must have blocked until the child wrote');
        } finally {
            $stderr = \is_resource($pipes[2] ?? null) ? stream_get_contents($pipes[2]) : '';
            proc_close($process);
            self::assertSame('', $stderr, 'Writer process must not error');
        }
    }
}
