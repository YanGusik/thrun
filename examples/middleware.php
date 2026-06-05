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
                throw new \Exception("Custom error");
            },
        ],
        options: new WorkerOptions(threads: 1, concurrency: 1,
            middleware: [new \Thrun\Middleware\CatchMessageMiddleware()]),
    ),
    options: new SupervisorOptions(),
);

$supervisor->run();

/**
 * EXPECTED:
 * php ./examples/middleware.php
 * [WorkerThread][Thrun\Tests\Fixture\SendEmailMessage:one] Exception: Custom error (middleware.php:38)
 * [WorkerThread][Thrun\Tests\Fixture\SendEmailMessage:two] Exception: Custom error (middleware.php:38)
 * [WorkerThread][Thrun\Tests\Fixture\SendEmailMessage:three] Exception: Custom error (middleware.php:38)
 * [WorkerThread][Thrun\Tests\Fixture\SendEmailMessage:one] Exception: Custom error (middleware.php:38)
 * [WorkerThread][Thrun\Tests\Fixture\SendEmailMessage:two] Exception: Custom error (middleware.php:38)
 * [WorkerThread][Thrun\Tests\Fixture\SendEmailMessage:three] Exception: Custom error (middleware.php:38)
 * [WorkerThread][Thrun\Tests\Fixture\SendEmailMessage:one] Exception: Custom error (middleware.php:38)
 * [WorkerThread][Thrun\Tests\Fixture\SendEmailMessage:two] Exception: Custom error (middleware.php:38)
 * [WorkerThread][Thrun\Tests\Fixture\SendEmailMessage:three] Exception: Custom error (middleware.php:38)
 */