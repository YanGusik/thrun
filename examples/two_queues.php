<?php

declare(strict_types=1);

require_once __DIR__ . '/../vendor/autoload.php';

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

// emails: 6 сообщений от разных пользователей
$emails->send(Envelope::wrap(new SendEmailMessage('alice@example.com'))->with(new PartitionStamp('user_1')));
$emails->send(Envelope::wrap(new SendEmailMessage('bob@example.com'))->with(new PartitionStamp('user_2')));
$emails->send(Envelope::wrap(new SendEmailMessage('alice@example.com'))->with(new PartitionStamp('user_1')));
$emails->send(Envelope::wrap(new SendEmailMessage('bob@example.com'))->with(new PartitionStamp('user_2')));
$emails->send(Envelope::wrap(new SendEmailMessage('carol@example.com'))->with(new PartitionStamp('user_3')));
$emails->send(Envelope::wrap(new SendEmailMessage('carol@example.com'))->with(new PartitionStamp('user_3')));

// notifications: 2 сообщения
$notifications->send(Envelope::wrap(new PushNotificationMessage(userId: 1))->with(new PartitionStamp('user_1')));
$notifications->send(Envelope::wrap(new PushNotificationMessage(userId: 2))->with(new PartitionStamp('user_2')));

// emails приоритет 3, notifications приоритет 1 -> пропорция 3:1
// max 2 активных задачи на одного пользователя
$supervisor = new Supervisor(
    workerFactory: fn() => new Worker(
        transport: new PolicyAwareReceiver(
            inner: new MultiQueueReceiver(
                receivers: [
                    'emails'        => $emails,
                    'notifications' => $notifications,
                ],
                strategy:   new \Thrun\Transport\Strategy\RoundRobinStrategy(),
                priorities: ['emails' => 3, 'notifications' => 1],
            ),
            policy: new \Thrun\Transport\Policy\ChainPolicy([]),
//            policy: new MaxConcurrencyPolicy(maxPerPartition: 1),
        ),
        handlers: [
            SendEmailMessage::class        => static fn(SendEmailMessage $m)        => print("email → {$m->to}\n"),
            PushNotificationMessage::class => static fn(PushNotificationMessage $m) => print("push  → user {$m->userId}\n"),
        ],
        options: new WorkerOptions(threads: 1, concurrency: 1),
    ),
    options: new SupervisorOptions(),
);

$supervisor->run();
