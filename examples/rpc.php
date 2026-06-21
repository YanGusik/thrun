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
$rpcServer = new RpcServer($serverSocket, $serializer);


$emails = new InMemoryTransport();
$rpcServer->registerLocalQueue('emails', $emails);

$rpcServer->registerRpcHandler('ping', fn(array $args) => ['pong' => true, 'echo' => $args]);

spawn(function () use ($rpcServer): void {
    $rpcServer->run();
});

delay(100);

echo "=== Test 1: Event broadcast ===\n";

$subscriber = stream_socket_client("unix://{$socketPath}");
FrameStream::write($subscriber, Frame::subscribe('order.completed'));
delay(50);

$publisher = stream_socket_client("unix://{$socketPath}");
FrameStream::write($publisher, Frame::event('order.completed', ['order_id' => 123]));

$received = FrameStream::read($subscriber);
echo ($received?->payload['data']['order_id'] ?? null) === 123
    ? "OK: subscriber got the event\n"
    : "FAIL: event not received\n";

fclose($subscriber);
fclose($publisher);



echo "=== Test 2: RPC request/reply ===\n";

$rpcClient = stream_socket_client("unix://{$socketPath}");
$correlationId = bin2hex(random_bytes(8));
FrameStream::write($rpcClient, Frame::request($correlationId, 'ping', ['hello' => 'world']));

$reply = FrameStream::read($rpcClient);
echo $reply?->type === FrameType::RpcReply
&& $reply->payload['correlationId'] === $correlationId
&& $reply->payload['result']['pong'] === true
    ? "OK: got correct rpc reply\n"
    : "FAIL: rpc reply mismatch\n";

var_dump('rpc payload:',$reply->payload);

fclose($rpcClient);

echo "=== Test 3: Job routed to local channel ===\n";

$jobClient = stream_socket_client("unix://{$socketPath}");
$envelopeJson = $serializer->serialize(Envelope::wrap((object) ['type' => 'PingMessage']));
FrameStream::write($jobClient, Frame::job('emails', $envelopeJson));
delay(50);

$envelope = $emails->tryReceive();
echo $envelope !== null
    ? "OK: job arrived in local channel\n"
    : "FAIL: job did not arrive\n";

fclose($jobClient);

$rpcServer->stop();
@unlink($socketPath);

echo "Done.\n";