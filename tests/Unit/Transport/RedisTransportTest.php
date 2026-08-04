<?php

declare(strict_types=1);

namespace Thrun\Tests\Unit\Transport;

use Testo\Assert;
use Testo\Lifecycle\BeforeClass;
use Testo\Lifecycle\BeforeTest;
use Testo\Test;
use Thrun\Envelope\Envelope;
use Thrun\Envelope\Stamp\ErrorDetailsStamp;
use Thrun\Envelope\UnprocessableMessage;
use Thrun\Serialization\ClassMapMessageTypeResolver;
use Thrun\Serialization\JsonSerializer;
use Thrun\Envelope\Stamp\DelayStamp;
use Thrun\Tests\AsyncTestCase;
use Thrun\Tests\Fixture\PingMessage;
use Thrun\Tests\Fixture\SendEmailMessage;
use Thrun\Transport\Redis\RedisConnection;
use Thrun\Transport\Redis\RedisStamp;
use Thrun\Transport\Redis\RedisTransport;

final class RedisTransportTest extends AsyncTestCase
{
    private static ?\Redis $redis = null;
    private RedisConnection $connection;
    private JsonSerializer $serializer;

    #[BeforeClass]
    public static function connectRedis(): void
    {
        $redis = new \Redis();

        $hosts = ['127.0.0.1:6379', 'redis:6379'];
        $connected = false;

        foreach ($hosts as $hostPort) {
            [$host, $port] = explode(':', $hostPort);
            try {
                $redis->connect($host, (int) $port, 1);
                $connected = true;
                break;
            } catch (\RedisException) {
                continue;
            }
        }

        if (!$connected) {
            throw new \RuntimeException('Redis is not available. Run: docker compose up redis -d');
        }

        self::$redis = $redis;
    }

    #[BeforeTest]
    public function setUpTest(): void
    {
        if (self::$redis === null) {
            throw new \RuntimeException('Redis not connected');
        }

        $this->connection = new RedisConnection(self::$redis, 'thrun:test');
        $this->connection->purge('default');

        $resolver = new ClassMapMessageTypeResolver();
        $this->serializer = new JsonSerializer($resolver);
    }

    public function sendPushesToReady(): void
    {
        $transport = new RedisTransport($this->connection, $this->serializer, 'default');
        $transport->send(Envelope::wrap(new PingMessage()));

        $len = self::$redis->lLen('thrun:test:default:ready');
        Assert::same($len, 1);
    }

    public function receiveDeserializesEnvelope(): void
    {
        $transport = new RedisTransport($this->connection, $this->serializer, 'default');
        $transport->send(Envelope::wrap(new SendEmailMessage('to@test.com', 'Hello')));

        $envelope = $transport->receive();

        Assert::same($envelope->message::class, SendEmailMessage::class);
        Assert::same($envelope->message->to, 'to@test.com');
        Assert::same($envelope->message->subject, 'Hello');
    }

    public function receiveAddsRedisStamp(): void
    {
        $transport = new RedisTransport($this->connection, $this->serializer, 'default');
        $transport->send(Envelope::wrap(new PingMessage()));

        $envelope = $transport->receive();
        $stamp = $envelope->last(RedisStamp::class);

        Assert::true($stamp instanceof RedisStamp);
        Assert::same($stamp->queue, 'default');
        Assert::same($stamp->rawPayload !== '', true);
    }

    public function ackRemovesFromProcessing(): void
    {
        $transport = new RedisTransport($this->connection, $this->serializer, 'default');
        $transport->send(Envelope::wrap(new PingMessage()));

        $envelope = $transport->receive();
        $transport->ack($envelope);

        $processingLen = self::$redis->lLen('thrun:test:default:processing');
        Assert::same($processingLen, 0);
    }

    public function rejectRemovesFromProcessingOnly(): void
    {
        $transport = new RedisTransport($this->connection, $this->serializer, 'default');
        $transport->send(Envelope::wrap(new PingMessage()));

        $envelope = $transport->receive();
        $transport->reject($envelope);

        $processingLen = self::$redis->lLen('thrun:test:default:processing');
        $failedLen     = self::$redis->lLen('thrun:test:default:failed');

        Assert::same($processingLen, 0);
        Assert::same($failedLen, 0);
    }

    public function startupReclaimReturnsStuckMessages(): void
    {
        // Simulate a stuck message from a previous crashed worker
        $raw = $this->serializer->serialize(Envelope::wrap(new PingMessage()));
        self::$redis->lPush('thrun:test:default:processing', $raw);

        // Creating a new transport should reclaim it
        new RedisTransport($this->connection, $this->serializer, 'default');

        $readyLen      = self::$redis->lLen('thrun:test:default:ready');
        $processingLen = self::$redis->lLen('thrun:test:default:processing');

        Assert::same($readyLen, 1);
        Assert::same($processingLen, 0);
    }

    public function invalidPayloadIsWrappedAsUnprocessable(): void
    {
        // Push invalid JSON directly to ready
        self::$redis->rPush('thrun:test:default:ready', 'not valid json');

        $transport = new RedisTransport($this->connection, $this->serializer, 'default');
        $envelope = $transport->tryReceive();

        // A payload that fails to deserialize is handed over as an
        // UnprocessableMessage carrying the raw text, instead of being dropped
        // inside the transport.
        Assert::instanceOf($envelope?->message, UnprocessableMessage::class);
        Assert::same($envelope->message->rawPayload, 'not valid json');
        Assert::same($envelope->message->queue, 'default');

        $error = $envelope->last(ErrorDetailsStamp::class);
        Assert::instanceOf($error, ErrorDetailsStamp::class);
        Assert::same($error->message !== '', true);

        // The raw payload travels on a RedisStamp, which is what ack() and
        // reject() need to find the entry again.
        $redisStamp = $envelope->last(RedisStamp::class);
        Assert::instanceOf($redisStamp, RedisStamp::class);
        Assert::same($redisStamp->rawPayload, 'not valid json');

        // The transport no longer writes a failed list of its own; the entry
        // stays in processing until the worker acks or rejects it.
        Assert::same(self::$redis->lLen('thrun:test:default:failed'), 0);
        Assert::same(self::$redis->lLen('thrun:test:default:processing'), 1);

        $transport->reject($envelope);
    }

    public function closeStopsReceive(): void
    {
        $transport = new RedisTransport($this->connection, $this->serializer, 'default');

        // Start receive in background
        $coro = \Async\spawn(function () use ($transport): ?Envelope {
            return $transport->receive();
        });

        // Close should stop receive()
        \Async\delay(100);
        $transport->close();

        try {
            $result = \Async\await($coro);
            Assert::same($result, null);
        } catch (\Cancellation) {
            // also fine
            Assert::same(true, true);
        }
    }

    public function delayedMessageGoesToSortedSet(): void
    {
        $transport = new RedisTransport($this->connection, $this->serializer, 'default');
        $transport->send(Envelope::wrap(new PingMessage(), new DelayStamp(5000)));

        $readyLen = self::$redis->lLen('thrun:test:default:ready');
        $delayedLen = self::$redis->zCard('thrun:test:default:delayed');

        Assert::same($readyLen, 0);
        Assert::same($delayedLen, 1);
    }

    public function delayedMessageReleasedAfterDelay(): void
    {
        $transport = new RedisTransport($this->connection, $this->serializer, 'default');
        $transport->send(Envelope::wrap(new PingMessage(), new DelayStamp(100)));

        // Immediately: should be in delayed, not ready
        Assert::same(self::$redis->lLen('thrun:test:default:ready'), 0);
        Assert::same(self::$redis->zCard('thrun:test:default:delayed'), 1);

        // Wait for delay to pass
        \Async\delay(200);

        // After receive() triggers releaseDelayed, message should move to ready
        $envelope = $transport->tryReceive();
        Assert::same($envelope !== null, true);
        Assert::same($envelope->message::class, PingMessage::class);

        Assert::same(self::$redis->zCard('thrun:test:default:delayed'), 0);
    }
}
