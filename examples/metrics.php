<?php

declare(strict_types=1);

require_once __DIR__.'/../vendor/autoload.php';

use Thrun\Envelope\Envelope;
use Thrun\Envelope\Stamp\RetryStamp;
use Thrun\Supervisor\Supervisor;
use Thrun\Supervisor\SupervisorOptions;
use Thrun\Tests\Fixture\SendEmailMessage;
use Thrun\Transport\InMemory\InMemoryTransport;
use Thrun\Worker\Metrics\InMemoryMetrics;
use Thrun\Worker\Retry\FixedDelayStrategy;
use Thrun\Worker\Worker;
use Thrun\Worker\WorkerOptions;

$transport = new InMemoryTransport();
$metrics = new InMemoryMetrics();

for ($i = 1; $i <= 10; $i++) {
    $transport->send(Envelope::wrap(
        new SendEmailMessage("user{$i}@example.com", 'Welcome'),
        new RetryStamp(strategy: new FixedDelayStrategy(delayMs: 2000, maxAttempts: 2)),
    ));
}

$supervisor = new Supervisor(
    workerFactory: fn() => new Worker(
        transport: $transport,
        handlers: [
            SendEmailMessage::class => function (SendEmailMessage $m) {
                if (rand(1, 100) <= 30) {
                    throw new \RuntimeException('Temporary failure');
                }
            },
        ],
        options: new WorkerOptions(threads: 2, concurrency: 3),
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
        "\r\033[K  \033[32mprocessed: %-3d\033[0m  \033[31mfailed: %-3d\033[0m  \033[33mretried: %-3d\033[0m  \033[36mtimed out: %-3d\033[0m  avg: %s",
        $processed,
        $failed,
        $retried,
        $timedOut,
        $avgStr,
    );

    echo $line;
}
