<?php

declare(strict_types=1);

require_once __DIR__.'/../vendor/autoload.php';

use Thrun\Envelope\Envelope;
use Thrun\Envelope\Stamp\PartitionStamp;
use Thrun\Envelope\Stamp\RetryStamp;
use Thrun\Supervisor\Supervisor;
use Thrun\Supervisor\SupervisorOptions;
use Thrun\Tests\Fixture\PushNotificationMessage;
use Thrun\Tests\Fixture\SendEmailMessage;
use Thrun\Transport\InMemory\InMemoryTransport;
use Thrun\Transport\MultiQueueReceiver;
use Thrun\Transport\Policy\MaxConcurrencyPolicy;
use Thrun\Transport\PolicyAwareReceiver;
use Thrun\Transport\Strategy\PriorityStrategy;
use Thrun\Worker\Acknowledger;
use Thrun\Worker\Retry\FixedDelayStrategy;
use Thrun\Worker\Worker;
use Thrun\Worker\WorkerOptions;

$emails = new InMemoryTransport();

$emails->send(Envelope::wrap(new SendEmailMessage('one@example.com', 'Hello'),
    new RetryStamp(strategy: new FixedDelayStrategy(1000, 3))));
$emails->send(Envelope::wrap(new SendEmailMessage('two@example.com', 'Hello'),
    new RetryStamp(strategy: new FixedDelayStrategy(1000, 3))));
$emails->send(Envelope::wrap(new SendEmailMessage('three@example.com', 'Hello'),
    new RetryStamp(strategy: new FixedDelayStrategy(1000, 3))));

$supervisor = new Supervisor(
    workerFactory: fn() => new Worker(
        transport: $emails,
        handlers: [
            SendEmailMessage::class => function (SendEmailMessage $m, Acknowledger $ack) {
                $attempt = $ack->envelope->last(RetryStamp::class)?->attempts ?? 0;

                $t = date("H:i:s");
                $random = rand(1, 2);
                if ($random === 1) {
                    print("[$t][Email][times:$attempt] - {$m->to} error\n");
                    throw new \Exception("Custom error");
                }
                print("[$t][Email][times:$attempt] - {$m->to} processed\n");
            },
        ],
        options: new WorkerOptions(threads: 1, concurrency: 1),
    ),
    options: new SupervisorOptions(),
);

$supervisor->run();

/**
 * EXPECTED:
 * php ./examples/retry.php
 * [13:44:55][Email][times:0] - one@example.com error
 * [13:44:55][Email][times:0] - two@example.com error
 * [13:44:55][Email][times:0] - three@example.com error
 * [13:44:56][Email][times:1] - one@example.com error
 * [13:44:56][Email][times:1] - two@example.com error
 * [13:44:56][Email][times:1] - three@example.com error
 * [13:44:57][Email][times:2] - one@example.com error
 * [13:44:57][Email][times:2] - two@example.com processed
 * [13:44:57][Email][times:2] - three@example.com processed
 * [13:44:58][Email][times:3] - one@example.com processed
 *
 * php ./examples/retry.php
 * [13:45:13][Email][times:0] - one@example.com error
 * [13:45:13][Email][times:0] - two@example.com error
 * [13:45:13][Email][times:0] - three@example.com error
 * [13:45:14][Email][times:1] - one@example.com processed
 * [13:45:14][Email][times:1] - two@example.com error
 * [13:45:14][Email][times:1] - three@example.com error
 * [13:45:15][Email][times:2] - two@example.com error
 * [13:45:15][Email][times:2] - three@example.com processed
 * [13:45:16][Email][times:3] - two@example.com error
 */