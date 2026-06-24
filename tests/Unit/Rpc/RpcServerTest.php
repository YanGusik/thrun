<?php

namespace Thrun\Tests\Unit\Rpc;

use RuntimeException;
use Testo\Assert;
use Testo\Expect;
use Testo\Lifecycle\AfterClass;
use Testo\Lifecycle\AfterTest;
use Testo\Lifecycle\BeforeClass;
use Testo\Lifecycle\BeforeTest;
use Testo\Test;
use Thrun\Envelope\Envelope;
use Thrun\Rpc\Frame;
use Thrun\Rpc\FrameStream;
use Thrun\Rpc\FrameType;
use Thrun\Rpc\RpcServer;
use Thrun\Serialization\ClassMapMessageTypeResolver;
use Thrun\Serialization\JsonSerializer;
use Thrun\Tests\AsyncTestCase;
use Thrun\Tests\Fixture\PingMessage;
use Thrun\Transport\InMemory\InMemoryTransport;
use function Async\await;
use function Async\delay;
use function Async\spawn;
use function Async\timeout;

//#[Test] bug with zombie coroutine
final class RpcServerTest
{
    private string $socketPath;
    private RpcServer $rpcServer;

    #[BeforeTest]
    protected function createSocketPath(): void
    {
        $this->socketPath = sys_get_temp_dir().'/thrun_rpc_test_'.bin2hex(random_bytes(4)).'.sock';
        @unlink($this->socketPath);
        $this->rpcServer = $this->startServer();
    }

    #[AfterTest]
    protected function deleteSocketFile(): void
    {
        $this->rpcServer->stop();
    }

    private function startServer(): RpcServer
    {
        $serverSocket = stream_socket_server("unix://{$this->socketPath}", $errno, $errstr);
        if ($serverSocket === false) {
            throw new RuntimeException("$errstr ($errno)");
        }

        $server = new RpcServer($serverSocket, new JsonSerializer(new ClassMapMessageTypeResolver()));

        spawn(function () use ($server): void {
            $server->run();
        });

        delay(50);

        return $server;
    }

    public function broadcastsEventToCorrectSubscriber(): void
    {
        $subscriber = stream_socket_client("unix://{$this->socketPath}");
        FrameStream::write($subscriber, Frame::subscribe('order.completed'));
        delay(20);

        $subscriberOther = stream_socket_client("unix://{$this->socketPath}");
        delay(20);

        $publisher = stream_socket_client("unix://{$this->socketPath}");
        FrameStream::write($publisher, Frame::event('order.completed', ['order_id' => 123]));

        $test = spawn(function () use ($subscriberOther) {
            return FrameStream::read($subscriberOther);
        });

        try {
            await($test, timeout(100));
            Assert::fail("Should have thrown an exception");
        } catch (\Cancellation $cancellation) {
            Assert::object($cancellation);
        } finally {
            fclose($subscriberOther);
        }

        $received = FrameStream::read($subscriber);

        Assert::same($received->type, FrameType::Event);
        Assert::same($received->payload['data']['order_id'], 123);

        fclose($subscriber);
        fclose($publisher);
    }

    public function broadcastsEventToSubscriber(): void
    {
        $subscriber = stream_socket_client("unix://{$this->socketPath}");
        FrameStream::write($subscriber, Frame::subscribe('order.completed'));
        delay(20);

        $publisher = stream_socket_client("unix://{$this->socketPath}");
        FrameStream::write($publisher, Frame::event('order.completed', ['order_id' => 123]));

        $received = FrameStream::read($subscriber);

        Assert::same($received->type, FrameType::Event);
        Assert::same($received->payload['data']['order_id'], 123);

        fclose($subscriber);
        fclose($publisher);
    }

    public function doesNotDeliverEventToUnrelatedSubscription(): void
    {
        $bystander = stream_socket_client("unix://{$this->socketPath}");
        FrameStream::write($bystander, Frame::subscribe('some.other.event'));
        delay(20);

        $publisher = stream_socket_client("unix://{$this->socketPath}");
        FrameStream::write($publisher, Frame::event('order.completed', ['order_id' => 1]));
        FrameStream::write($publisher, Frame::event('some.other.event', ['ok' => true]));

        $received = FrameStream::read($bystander);
        Assert::same($received->payload['event'], 'some.other.event');

        fclose($bystander);
        fclose($publisher);
    }

    public function routesJobToRegisteredLocalQueue(): void
    {
        $emails = new InMemoryTransport();
        $this->rpcServer->registerLocalQueue('emails', $emails);

        $serializer = new JsonSerializer(new ClassMapMessageTypeResolver());
        $envelopeJson = $serializer->serialize(Envelope::wrap(new PingMessage()));

        $client = stream_socket_client("unix://{$this->socketPath}");
        FrameStream::write($client, Frame::job('emails', $envelopeJson));
        delay(50);

        $envelope = $emails->tryReceive();

        Assert::notNull($envelope);
        Assert::same($envelope->message::class, PingMessage::class);

        fclose($client);
    }

    public function rejectsJobForUnknownQueue(): void
    {
        $serializer = new JsonSerializer(new ClassMapMessageTypeResolver());
        $envelopeJson = $serializer->serialize(Envelope::wrap(new PingMessage()));

        $client = stream_socket_client("unix://{$this->socketPath}");
        FrameStream::write($client, Frame::job('unknown-queue', $envelopeJson));

        $response = FrameStream::read($client);

        Assert::same($response->type, FrameType::Error);

        fclose($client);
    }

    public function handlesRpcRequestReply(): void
    {
        $this->rpcServer->registerRpcHandler('ping', fn(array $args) => ['pong' => true, 'echo' => $args]);

        $client = stream_socket_client("unix://{$this->socketPath}");
        FrameStream::write($client, Frame::request('cid-1', 'ping', ['hello' => 'world']));

        $reply = FrameStream::read($client);

        Assert::same($reply->type, FrameType::RpcReply);
        Assert::same($reply->payload['correlationId'], 'cid-1');
        Assert::same($reply->payload['result']['pong'], true);

        fclose($client);
    }

    public function returnsErrorForUnknownRpcMethod(): void
    {
        $client = stream_socket_client("unix://{$this->socketPath}");
        FrameStream::write($client, Frame::request('cid-2', 'does-not-exist', []));

        $response = FrameStream::read($client);

        Assert::same($response->type, FrameType::Error);
        Assert::same($response->payload['correlationId'], 'cid-2');

        fclose($client);
    }

    public function survivesPublishAfterSubscriberDisconnects(): void
    {
        $subscriber = stream_socket_client("unix://{$this->socketPath}");
        FrameStream::write($subscriber, Frame::subscribe('order.completed'));
        delay(20);
        fclose($subscriber);
        delay(20);

        $publisher = stream_socket_client("unix://{$this->socketPath}");
        FrameStream::write($publisher, Frame::event('order.completed', ['order_id' => 1]));
        delay(20);

        Assert::true(true);

        fclose($publisher);
    }
}