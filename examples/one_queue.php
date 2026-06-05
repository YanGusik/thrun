<?php

declare(strict_types=1);

require_once __DIR__.'/../vendor/autoload.php';

use Thrun\Envelope\Envelope;
use Thrun\Supervisor\Supervisor;
use Thrun\Supervisor\SupervisorOptions;
use Thrun\Tests\Fixture\SendEmailMessage;
use Thrun\Transport\InMemory\InMemoryTransport;
use Thrun\Worker\Worker;
use Thrun\Worker\WorkerOptions;

$emails = new InMemoryTransport();

$emails->send(Envelope::wrap(new SendEmailMessage('one@example.com', 'Hello')));
$emails->send(Envelope::wrap(new SendEmailMessage('two@example.com', 'Hello')));
$emails->send(Envelope::wrap(new SendEmailMessage('three@example.com', 'Hello')));
$emails->send(Envelope::wrap(new SendEmailMessage('four@example.com', 'Hello')));
$emails->send(Envelope::wrap(new SendEmailMessage('five@example.com', 'Hello')));
$emails->send(Envelope::wrap(new SendEmailMessage('six@example.com', 'Hello')));

$supervisor = new Supervisor(
    workerFactory: fn() => new Worker(
        transport: $emails,
        // If you need policy, use PolicyAwareReceiver
        //        transport: new PolicyAwareReceiver(
        //            inner: $emails,
        //            policy: new \Thrun\Transport\Policy\MaxConcurrencyPolicy([]),
        //        ),
        handlers: [
            SendEmailMessage::class => fn(SendEmailMessage $m) => print("[Email] - {$m->to} processed\n"),
        ],
        options: new WorkerOptions(threads: 1, concurrency: 1),
    ),
    options: new SupervisorOptions(),
);

$supervisor->run();