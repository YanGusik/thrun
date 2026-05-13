<?php

declare(strict_types=1);

require_once __DIR__.'/../vendor/autoload.php';

use Thrun\Envelope\Envelope;
use Thrun\Serialization\ClassMapMessageTypeResolver;
use Thrun\Serialization\JsonSerializer;
use Thrun\Supervisor\Supervisor;
use Thrun\Supervisor\SupervisorOptions;
use Thrun\Tests\Fixture\SendEmailMessage;
use Thrun\Transport\Redis\RedisConnection;
use Thrun\Transport\Redis\RedisTransport;
use Thrun\Worker\Worker;
use Thrun\Worker\WorkerOptions;

$redis = new \Redis();
$connected = false;
foreach (['redis:6379', '127.0.0.1:6379'] as $hostPort) {
    [$host, $port] = explode(':', $hostPort);
    try {
        $redis->connect($host, (int) $port, 1);
        $connected = true;
        echo "Connected to Redis at {$hostPort}\n";
        break;
    } catch (\RedisException) {
        continue;
    }
}

if (!$connected) {
    throw new \RuntimeException('Redis is not available');
}

$connection = new RedisConnection($redis, 'thrun:example');
$connection->purge('emails');

$transport = new RedisTransport(
    $connection,
    new JsonSerializer(new ClassMapMessageTypeResolver()),
    'emails',
);

$transport->send(Envelope::wrap(new SendEmailMessage('one@example.com', 'Hello')));
$transport->send(Envelope::wrap(new SendEmailMessage('two@example.com', 'Hello')));
$transport->send(Envelope::wrap(new SendEmailMessage('three@example.com', 'Hello')));
$transport->send(Envelope::wrap(new SendEmailMessage('four@example.com', 'Hello')));
$transport->send(Envelope::wrap(new SendEmailMessage('five@example.com', 'Hello')));
$transport->send(Envelope::wrap(new SendEmailMessage('six@example.com', 'Hello')));

$supervisor = new Supervisor(
    workerFactory: fn() => new Worker(
        transport: $transport,
        handlers: [
            SendEmailMessage::class => fn(SendEmailMessage $m) => print("[Email] - {$m->to} processed\n"),
        ],
        options: new WorkerOptions(threads: 1, concurrency: 1),
    ),
    options: new SupervisorOptions(),
);

$supervisor->run();
