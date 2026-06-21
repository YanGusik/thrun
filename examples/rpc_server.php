<?php

declare(strict_types=1);

require_once __DIR__.'/../vendor/autoload.php';

use Thrun\Envelope\Envelope;
use Thrun\Rpc\Frame;
use Thrun\Rpc\FrameStream;
use Thrun\Rpc\FrameType;
use Thrun\Rpc\RpcServer;
use Thrun\Serialization\ClassMapMessageTypeResolver;
use Thrun\Serialization\JsonSerializer;
use Thrun\Supervisor\Supervisor;
use Thrun\Supervisor\SupervisorOptions;
use Thrun\Tests\Fixture\SendEmailMessage;
use Thrun\Transport\InMemory\InMemoryTransport;
use Thrun\Worker\Worker;
use Thrun\Worker\WorkerOptions;
use function Async\delay;
use function Async\spawn;


$socketPath = sys_get_temp_dir().'/thrun_rpc_test.sock';
@unlink($socketPath);
$serverSocket = stream_socket_server("unix://{$socketPath}", $errno, $errstr);
if ($serverSocket === false) {
    throw new RuntimeException("bind failed: {$errstr}");
}

$serializer = new JsonSerializer(new ClassMapMessageTypeResolver());
$rpcServer  = new RpcServer($serverSocket, $serializer);


$emails = new InMemoryTransport();
$rpcServer->registerLocalQueue('emails', $emails);

$rpcServer->registerRpcHandler('ping', fn(array $args) => ['pong' => true, 'echo' => $args]);

spawn(function () use ($rpcServer): void {
    $rpcServer->run();
});

delay(100);

$supervisor = new Supervisor(
    workerFactory: fn() => new Worker(
        transport: $emails,
        handlers: [
            SendEmailMessage::class => static function (SendEmailMessage $m) use ($socketPath): void {
                delay(100);

                $publisher = \Thrun\Tests\Fixture\ThreadLocalPublisher::get($socketPath);
                FrameStream::write($publisher, Frame::event('order.completed', ['order_id' => $m->to]));
                print("[Email] - {$m->to} processed from RPC\n");
            },
        ],
        options: new WorkerOptions(threads: 2, concurrency: 1),
    ),
    options: new SupervisorOptions(),
);

$supervisor->run();
$rpcServer->stop();
/**
 * create connect
 * create connect
 * [Email] - 1@example.com processed from RPC
 * [Email] - 0@example.com processed from RPC
 * [Email] - 3@example.com processed from RPC
 * [Email] - 2@example.com processed from RPC
 * [Email] - 4@example.com processed from RPC
 * [Email] - 5@example.com processed from RPC
 * [Email] - 7@example.com processed from RPC
 * [Email] - 6@example.com processed from RPC
 * [Email] - 9@example.com processed from RPC
 * [Email] - 8@example.com processed from RPC
 * ^C
 * [Supervisor] Signal SIGINT received. Stopping worker...
 */