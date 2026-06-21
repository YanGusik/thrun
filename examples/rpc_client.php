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
use function Async\delay;


$socketPath = sys_get_temp_dir().'/thrun_rpc_test.sock';
$serializer = new JsonSerializer(new ClassMapMessageTypeResolver());

$jobClient = stream_socket_client("unix://{$socketPath}");
for ($i = 0; $i < 10; $i++) {
    $tmp = $serializer->serialize(Envelope::wrap(new SendEmailMessage("$i@example.com", "Hello-$i")));
    FrameStream::write($jobClient, Frame::job('emails', $tmp));
}

$subscriber = stream_socket_client("unix://{$socketPath}");
FrameStream::write($subscriber, Frame::subscribe('order.completed'));
delay(50);

for ($i = 0; $i < 10; ++$i) {
    $received = FrameStream::read($subscriber);
    echo sprintf("[event] %s, [data.order_id] %s\n",$received?->payload['event'], $received?->payload['data']['order_id']);
}

fclose($subscriber);

/**
 * [event] order.completed, [data.order_id] 0@example.com
 * [event] order.completed, [data.order_id] 1@example.com
 * [event] order.completed, [data.order_id] 3@example.com
 * [event] order.completed, [data.order_id] 2@example.com
 * [event] order.completed, [data.order_id] 4@example.com
 * [event] order.completed, [data.order_id] 5@example.com
 * [event] order.completed, [data.order_id] 7@example.com
 * [event] order.completed, [data.order_id] 6@example.com
 * [event] order.completed, [data.order_id] 9@example.com
 * [event] order.completed, [data.order_id] 8@example.com
 */