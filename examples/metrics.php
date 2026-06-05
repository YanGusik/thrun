<?php

declare(strict_types=1);

require_once __DIR__.'/../vendor/autoload.php';

use Thrun\Envelope\Envelope;
use Thrun\Envelope\Stamp\MessageIdStamp;
use Thrun\Envelope\Stamp\RetryStamp;
use Thrun\Middleware\CatchMessageMiddleware;
use Thrun\Supervisor\Supervisor;
use Thrun\Supervisor\SupervisorOptions;
use Thrun\Tests\Fixture\SendEmailMessage;
use Thrun\Transport\InMemory\InMemoryTransport;
use Thrun\Worker\Acknowledger;
use Thrun\Worker\Metrics\InMemoryMetrics;
use Thrun\Worker\Worker;
use Thrun\Worker\WorkerOptions;

$transport = new InMemoryTransport();
$metrics = new InMemoryMetrics();

for ($i = 1; $i <= 10; $i++) {
    $transport->send(Envelope::wrap(
        new SendEmailMessage("user{$i}@example.com", 'Welcome'),
        new RetryStamp(backoff: [2000], maxAttempts: 2),
        new MessageIdStamp($i),
    ));
}

$supervisor = new Supervisor(
    workerFactory: fn() => new Worker(
        transport: $transport,
        handlers: [
            SendEmailMessage::class => function (SendEmailMessage $m, Acknowledger $ack) {
                if (rand(1, 100) <= 30) {
                    throw new \RuntimeException('Temporary failure');
                }
                $id = $ack->envelope->last(MessageIdStamp::class)?->id;
                echo "completed $id\n";
            },
        ],
        options: new WorkerOptions(threads: 2, concurrency: 3, middleware: [new CatchMessageMiddleware()]),
        metrics: $metrics,
    ),
    options: new SupervisorOptions(maxCrashes: 5),
);

// Auto-close transport after 15s
$closer = \Async\spawn(function () use ($transport): void {
    \Async\delay(15000);
    $transport->close();
});

// Live metrics reporter
$reporter = \Async\spawn(function () use ($metrics): void {
    while (true) {
        \Async\delay(1000);
        renderMetrics($metrics);
    }
});

echo "Thrun Metrics Live (stop: Ctrl+C or wait 15s)\n\n";

$supervisor->run();

$reporter->cancel();

// Final output on a fresh line
echo "\n";
echo "Done. Processed: {$metrics->processed}, Failed: {$metrics->failed}, Retried: {$metrics->retried}\n";

function renderMetrics(InMemoryMetrics $metrics): void
{
    $processed = $metrics->processed;
    $failed = $metrics->failed;
    $retried = $metrics->retried;
    $timedOut = $metrics->timedOut;
    $avg = $metrics->averageTime();

    $avgStr = $avg > 0.001
        ? round($avg, 3) . 's'
        : round($avg * 1000, 2) . 'ms';

    $line = sprintf(
        "\r\033[K  \033[32mprocessed: %-3d\033[0m  \033[31mfailed: %-3d\033[0m  \033[33mretried: %-3d\033[0m  \033[36mtimed out: %-3d\033[0m  avg: %s\n",
        $processed,
        $failed,
        $retried,
        $timedOut,
        $avgStr,
    );

    echo $line;
}


/**
 * Expected:
 *
 * completed 1
 * completed 4
 * completed 2
 * [WorkerThread][Thrun\Tests\Fixture\SendEmailMessage:5] RuntimeException: Temporary failure (metrics.php:37)
 * [WorkerThread][Thrun\Tests\Fixture\SendEmailMessage:3] RuntimeException: Temporary failure (metrics.php:37)
 * completed 6
 * completed 7
 * [WorkerThread][Thrun\Tests\Fixture\SendEmailMessage:8] RuntimeException: Temporary failure (metrics.php:37)
 * [WorkerThread][Thrun\Tests\Fixture\SendEmailMessage:9] RuntimeException: Temporary failure (metrics.php:37)
 * [WorkerThread][Thrun\Tests\Fixture\SendEmailMessage:10] RuntimeException: Temporary failure (metrics.php:37)
 * processed: 5    failed: 5    retried: 5    timed out: 0    avg: 0.04ms
 * completed 5
 * completed 3
 * completed 8
 * completed 10
 * [WorkerThread][Thrun\Tests\Fixture\SendEmailMessage:9][1:2026-06-05T11:15:06+00:00] RuntimeException: Temporary failure (metrics.php:37)
 * processed: 9    failed: 6    retried: 5    timed out: 0    avg: 0.07ms
 */