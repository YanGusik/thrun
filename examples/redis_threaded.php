<?php

declare(strict_types=1);

require_once __DIR__.'/../vendor/autoload.php';

use Thrun\Envelope\Envelope;
use Thrun\Serialization\ClassMapMessageTypeResolver;
use Thrun\Serialization\JsonSerializer;
use Thrun\Supervisor\Supervisor;
use Thrun\Supervisor\SupervisorOptions;
use Thrun\Transport\Redis\RedisConnection;
use Thrun\Transport\Redis\RedisTransport;
use Thrun\Worker\Acknowledger;
use Thrun\Worker\Worker;
use Thrun\Worker\WorkerOptions;
use Thrun\Worker\Metrics\InMemoryMetrics;

// Simple CPU-bound message
final class CpuJob
{
    public function __construct(public readonly int $iterations = 500) {}
}

$redis = new \Redis();
$connected = false;
foreach (['redis:6379', '127.0.0.1:6379'] as $hostPort) {
    [$host, $port] = explode(':', $hostPort);
    try {
        $redis->connect($host, (int) $port, 1);
        $connected = true;
        echo "Connected to Redis at {$hostPort}\n";
        break;
    } catch (\RedisException) {
        continue;
    }
}

if (!$connected) {
    throw new \RuntimeException('Redis is not available');
}

$connection = new RedisConnection($redis, 'thrun:threaded');
$connection->purge('jobs');

$transport = new RedisTransport(
    $connection,
    new JsonSerializer(new ClassMapMessageTypeResolver()),
    'jobs',
);

$count = 20;
for ($i = 1; $i <= $count; $i++) {
    $transport->send(Envelope::wrap(new CpuJob(1_000_000)));
}
echo "Pushed {$count} CPU jobs\n";

$metrics = new InMemoryMetrics();

$supervisor = new Supervisor(
    workerFactory: fn() => new Worker(
        transport: $transport,
        handlers: [
            CpuJob::class => function (CpuJob $job, Acknowledger $ack): void {
                $x = 0.0;
                for ($i = 0; $i < $job->iterations; $i++) {
                    $x += sin($i) * cos($i);
                }
                $ack->ack();
            },
        ],
        options: new WorkerOptions(threads: 2, concurrency: 4),
        metrics: $metrics,
    ),
    options: new SupervisorOptions(),
);

// Reporter coroutine (like ConsoleReporter but simple)
$reporter = \Async\spawn(function () use ($metrics, $count): void {
    while (true) {
        \Async\delay(1000);
        $processed = $metrics->processed;
        $failed = $metrics->failed;
        echo sprintf("processed: %d  failed: %d  pending: %d\n", $processed, $failed, $count - $processed);
        if ($processed >= $count) {
            echo "All jobs done.\n";
            break;
        }
    }
});

$supervisor->run();
$reporter->cancel();
echo "Supervisor finished.\n";
