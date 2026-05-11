<?php

declare(strict_types=1);

require_once __DIR__.'/../vendor/autoload.php';

use Thrun\Envelope\Envelope;
use Thrun\Envelope\Stamp\PartitionStamp;
use Thrun\Supervisor\Supervisor;
use Thrun\Supervisor\SupervisorOptions;
use Thrun\Tests\Fixture\PushNotificationMessage;
use Thrun\Tests\Fixture\SendEmailMessage;
use Thrun\Transport\InMemory\InMemoryTransport;
use Thrun\Transport\MultiQueueReceiver;
use Thrun\Transport\Policy\ChainPolicy;
use Thrun\Transport\Policy\MaxConcurrencyPolicy;
use Thrun\Transport\PolicyAwareReceiver;
use Thrun\Transport\Strategy\PriorityStrategy;
use Thrun\Transport\Strategy\RoundRobinStrategy;
use Thrun\Worker\Worker;
use Thrun\Worker\WorkerOptions;

$emails        = new InMemoryTransport();
$notifications = new InMemoryTransport();

$emails->send(Envelope::wrap(new SendEmailMessage('one@example.com', 'Hello')));
$emails->send(Envelope::wrap(new SendEmailMessage('two@example.com', 'Hello')));
$emails->send(Envelope::wrap(new SendEmailMessage('three@example.com', 'Hello')));
$emails->send(Envelope::wrap(new SendEmailMessage('four@example.com', 'Hello')));
$emails->send(Envelope::wrap(new SendEmailMessage('five@example.com', 'Hello')));
$emails->send(Envelope::wrap(new SendEmailMessage('six@example.com', 'Hello')));

$notifications->send(Envelope::wrap(new PushNotificationMessage(userId: 1)));
$notifications->send(Envelope::wrap(new PushNotificationMessage(userId: 2)));

$supervisor = new Supervisor(
    workerFactory: fn() => new Worker(
        transport: new PolicyAwareReceiver(
            inner: new MultiQueueReceiver(
                receivers: [
                    'emails'        => $emails,
                    'notifications' => $notifications,
                ],
                strategy: new RoundRobinStrategy(),
                priorities: [],
            ),
            policy: new MaxConcurrencyPolicy(maxPerPartition: 2),
        ),
        handlers: [
            SendEmailMessage::class        => function (SendEmailMessage $m) {
                $time = date('H:i:s');
                print("[$time][Email] - {$m->to} pending\n");
                \Async\delay(1000);
                print("[$time][Email] - {$m->to} completed\n");
            },
            PushNotificationMessage::class => function (PushNotificationMessage $m) {
                $time = date('H:i:s');
                print("[$time][Notification] - {$m->userId} pending\n");
                \Async\delay(1000);
                print("[$time][Notification] - {$m->userId} completed\n");
            },
        ],
        options: new WorkerOptions(threads: 1, concurrency: 2),
    ),
    options: new SupervisorOptions(),
);

$supervisor->run();

/**
 * EXPECTED:
 * [10:29:46][Email] - one@example.com pending
 * [10:29:46][Notification] - 1 pending
 * [10:29:46][Email] - one@example.com completed
 * [10:29:46][Notification] - 1 completed
 * [10:29:47][Email] - two@example.com pending
 * [10:29:47][Notification] - 2 pending
 * [10:29:47][Email] - two@example.com completed
 * [10:29:47][Notification] - 2 completed
 * [10:29:48][Email] - three@example.com pending
 * [10:29:48][Email] - four@example.com pending
 * [10:29:48][Email] - three@example.com completed
 * [10:29:48][Email] - four@example.com completed
 * [10:29:52][Email] - five@example.com pending
 * [10:29:52][Email] - six@example.com pending
 * [10:29:52][Email] - five@example.com completed
 * [10:29:52][Email] - six@example.com completed
 */