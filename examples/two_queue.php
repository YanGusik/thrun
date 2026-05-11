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
use Thrun\Transport\Policy\MaxConcurrencyPolicy;
use Thrun\Transport\PolicyAwareReceiver;
use Thrun\Transport\Strategy\PriorityStrategy;
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
        transport: new MultiQueueReceiver(
            receivers: [
                'emails'        => $emails,
                'notifications' => $notifications,
            ],
            strategy: new \Thrun\Transport\Strategy\PriorityStrategy(),
            priorities: ['emails' => 3, 'notifications' => 1],
        ),
        handlers: [
            SendEmailMessage::class        => function (SendEmailMessage $m) {
                print("[Email] - {$m->to} processed\n");
            },
            PushNotificationMessage::class => function (PushNotificationMessage $m) {
                print("[Notification] - {$m->userId} processed\n");
            },
        ],
        options: new WorkerOptions(threads: 1, concurrency: 1),
    ),
    options: new SupervisorOptions(),
);

$supervisor->run();