<?php

declare(strict_types=1);

require_once __DIR__.'/../vendor/autoload.php';

use Thrun\Envelope\Envelope;
use Thrun\Envelope\Stamp\MessageIdStamp;
use Thrun\Envelope\Stamp\RetryStamp;
use Thrun\Supervisor\Supervisor;
use Thrun\Supervisor\SupervisorOptions;
use Thrun\Tests\Fixture\SendEmailMessage;
use Thrun\Transport\InMemory\InMemoryTransport;
use Thrun\Worker\Acknowledger;
use Thrun\Worker\Worker;
use Thrun\Worker\WorkerOptions;

$emails = new InMemoryTransport();

$emails->send(Envelope::wrap(new SendEmailMessage('one@example.com', 'Hello'),
    new RetryStamp(backoff: [1000], maxAttempts: 3), new MessageIdStamp('one')));
$emails->send(Envelope::wrap(new SendEmailMessage('two@example.com', 'Hello'),
    new RetryStamp(backoff: [1000], maxAttempts: 3), new MessageIdStamp('two')));
$emails->send(Envelope::wrap(new SendEmailMessage('three@example.com', 'Hello'),
    new RetryStamp(backoff: [1000, 2000, 3000], maxAttempts: 3), new MessageIdStamp('three')));

$supervisor = new Supervisor(
    workerFactory: fn() => new Worker(
        transport: $emails,
        handlers: [
            SendEmailMessage::class => function (SendEmailMessage $m, Acknowledger $ack) {
                $attempt = $ack->envelope->last(RetryStamp::class)?->attempts ?? 0;
                $id = $ack->envelope->last(MessageIdStamp::class)?->id ?? 0;

                $t      = date("H:i:s");
//                $random = rand(1, 2);
                $random = 1;
                if ($random === 1) {
                    print("[$t][Email][$id][times:$attempt] - {$m->to} error\n");
                    throw new \Exception("Custom error");
                }
                print("[$t][Email][$id][times:$attempt] - {$m->to} processed\n");
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