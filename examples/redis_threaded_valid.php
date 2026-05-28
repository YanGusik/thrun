<?php

declare(strict_types=1);

require_once __DIR__.'/../vendor/autoload.php';

use Thrun\Envelope\Envelope;
use Thrun\Serialization\ClassMapMessageTypeResolver;
use Thrun\Serialization\JsonSerializer;
use Thrun\Supervisor\Supervisor;
use Thrun\Supervisor\SupervisorOptions;
use Thrun\Tests\Fixture\SendEmailMessage;
use Thrun\Transport\Redis\RedisConnection;
use Thrun\Transport\Redis\RedisTransport;
use Thrun\Worker\Acknowledger;
use Thrun\Worker\Worker;
use Thrun\Worker\WorkerOptions;
use Thrun\Worker\Metrics\InMemoryMetrics;

$redis = new \Redis();
$connected = false;
foreach (['redis:6379', '127.0.0.1:6379'] as $hostPort) {
    [$host, $port] = explode(':', $hostPort);
    try {
        @$redis->connect($host, (int) $port, 1);
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

$connection = new RedisConnection($redis, 'thrun:threaded_valid');
$connection->purge('jobs');

$transport = new RedisTransport(
    $connection,
    new JsonSerializer(new ClassMapMessageTypeResolver()),
    'jobs',
);

$count = 20;
for ($i = 1; $i <= $count; $i++) {
    $transport->send(Envelope::wrap(new SendEmailMessage("job{$i}@test.com", "CPU job")));
}
echo "Pushed {$count} jobs\n";
$metrics = new InMemoryMetrics();

$supervisor = new Supervisor(
    workerFactory: fn() => new Worker(
        transport: $transport,
        handlers: [
            SendEmailMessage::class => function (SendEmailMessage $m, Acknowledger $ack) {
                $x = 0.0;
                for ($i = 0; $i < 20_000_000; $i++) {
                    $x += sin($i) * cos($i);
                }
                $ack->ack();
            },
        ],
        options: new WorkerOptions(threads: 1, concurrency: 1),
        metrics: $metrics,
    ),
    options: new SupervisorOptions(),
);

$reporter = \Async\spawn(function () use ($metrics, $transport, $count): void {
    $startPushed = $count;
    while (true) {
//        \Async\delay(1000);
        $pending   = $transport->pendingCount();
        $active    = $transport->activeCount();
        $processed = $metrics->processed;
        $failed    = $metrics->failed;
        echo sprintf(
            "pending: %d  active: %d  processed: %d  failed: %d  (this run pushed: %d)\n",
            $pending, $active, $processed, $failed, $startPushed,
        );
        // For one-shot test: stop when queue is empty and nothing is in-flight
        if ($pending === 0 && $active === 0 && $processed > 0) {
            echo "All jobs done.\n";
            break;
        }
        \Async\delay(1000);
    }
});

$supervisor->run();
//\Async\delay(1000);
//$reporter->cancel();
echo "Supervisor finished.\n";
