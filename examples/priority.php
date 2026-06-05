<?php

declare(strict_types=1);

require_once __DIR__.'/../vendor/autoload.php';

use Thrun\Envelope\Envelope;
use Thrun\Supervisor\Supervisor;
use Thrun\Supervisor\SupervisorOptions;
use Thrun\Tests\Fixture\PingMessage;
use Thrun\Tests\Fixture\PushNotificationMessage;
use Thrun\Tests\Fixture\SendEmailMessage;
use Thrun\Transport\InMemory\InMemoryTransport;
use Thrun\Transport\MultiQueueReceiver;
use Thrun\Transport\Policy\ChainPolicy;
use Thrun\Transport\PolicyAwareReceiver;
use Thrun\Transport\Strategy\PriorityStrategy;
use Thrun\Worker\Worker;
use Thrun\Worker\WorkerOptions;

$emails        = new InMemoryTransport();
$notifications = new InMemoryTransport();
$ping          = new InMemoryTransport();

$emails->send(Envelope::wrap(new SendEmailMessage('one@example.com', 'Hello')));
$emails->send(Envelope::wrap(new SendEmailMessage('two@example.com', 'Hello')));
$emails->send(Envelope::wrap(new SendEmailMessage('three@example.com', 'Hello')));
$emails->send(Envelope::wrap(new SendEmailMessage('four@example.com', 'Hello')));
$emails->send(Envelope::wrap(new SendEmailMessage('five@example.com', 'Hello')));
$emails->send(Envelope::wrap(new SendEmailMessage('six@example.com', 'Hello')));

$notifications->send(Envelope::wrap(new PushNotificationMessage(userId: 1)));
$notifications->send(Envelope::wrap(new PushNotificationMessage(userId: 2)));

$ping->send(Envelope::wrap(new PingMessage()));
$ping->send(Envelope::wrap(new PingMessage()));


$supervisor = new Supervisor(
    workerFactory: fn() => new Worker(
        transport: new MultiQueueReceiver(
            receivers: [
                'emails'        => $emails,
                'notifications' => $notifications,
                'ping'          => $ping,
            ],
            strategy: new PriorityStrategy(),
            priorities: ['emails' => 3, 'ping' => 2, 'notifications' => 1],
        ),
        handlers: [
            SendEmailMessage::class        => static function (SendEmailMessage $m) {
                print("[Email] - {$m->to} processed\n");
            },
            PushNotificationMessage::class => static function (PushNotificationMessage $m) {
                print("[Notification] - {$m->userId} processed\n");
            },
            PingMessage::class             => static function (PingMessage $m) {
                print("[Ping] Pong\n");
            },
        ],
        options: new WorkerOptions(threads: 1, concurrency: 1),
    ),
    options: new SupervisorOptions(),
);

$supervisor->run();



/**
 * EXPECTED:
 * [Email] - one@example.com processed
 * [Ping] Pong
 * [Email] - two@example.com processed
 * [Email] - three@example.com processed
 * [Ping] Pong
 * [Email] - four@example.com processed
 * [Email] - five@example.com processed
 * [Email] - six@example.com processed
 * [Notification] - 1 processed
 * [Notification] - 2 processed
 */