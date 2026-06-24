<?php

declare(strict_types=1);

require_once __DIR__.'/../vendor/autoload.php';

use Thrun\Envelope\Envelope;
use Thrun\Envelope\Stamp\PartitionStamp;
use Thrun\Supervisor\Supervisor;
use Thrun\Supervisor\SupervisorOptions;
use Thrun\Tests\Fixture\SendEmailMessage;
use Thrun\Transport\InMemory\InMemoryTransport;
use Thrun\Transport\Policy\MaxConcurrencyPolicy;
use Thrun\Transport\PolicyAwareReceiver;
use Thrun\Worker\Worker;
use Thrun\Worker\WorkerOptions;

$emails = new InMemoryTransport();

$emails->send(Envelope::wrap(new SendEmailMessage('one@example.com', 'Hello'), new PartitionStamp('one')));
$emails->send(Envelope::wrap(new SendEmailMessage('two@example.com', 'Hello'), new PartitionStamp('one')));
$emails->send(Envelope::wrap(new SendEmailMessage('three@example.com', 'Hello'), new PartitionStamp('two')));
$emails->send(Envelope::wrap(new SendEmailMessage('four@example.com', 'Hello'), new PartitionStamp('two')));
$emails->send(Envelope::wrap(new SendEmailMessage('five@example.com', 'Hello'), new PartitionStamp('three')));
$emails->send(Envelope::wrap(new SendEmailMessage('six@example.com', 'Hello'), new PartitionStamp('four')));
$emails->send(Envelope::wrap(new SendEmailMessage('seven@example.com', 'Hello'), new PartitionStamp('one')));

$supervisor = new Supervisor(
    workerFactory: fn() => new Worker(
        transport: new PolicyAwareReceiver(
            inner: $emails,
            policy: new MaxConcurrencyPolicy(1),
        ),
        handlers: [
            SendEmailMessage::class => fn(SendEmailMessage $m) => print("[Email] - {$m->to} processed\n"),
        ],
        options: new WorkerOptions(threads: 1, concurrency: 2),
    ),
    options: new SupervisorOptions(),
);

$supervisor->run();

/**
 * EXPECTED:
 * [Email] - one@example.com processed
 * [Email] - three@example.com processed
 * [Email] - five@example.com processed
 * [Email] - six@example.com processed
 * [Email] - two@example.com processed
 * [Email] - four@example.com processed
 * [Email] - seven@example.com processed
 */