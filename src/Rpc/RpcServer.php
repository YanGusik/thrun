<?php

declare(strict_types=1);

namespace Thrun\Rpc;

use Async\Scope;
use Async\TaskSet;
use Thrun\Contract\SerializerInterface;
use Thrun\Contract\TransportInterface;

final class RpcServer
{
    private bool $running = false;
    private Scope $scope;
    private TaskSet $tasks;

    /** @var array<string, TransportInterface> queue name => channel */
    private array $localQueues = [];

    /** @var array<string, callable> RPC method => handler */
    private array $rpcHandlers = [];

    /** @var array<string, array<int, resource>> event name => [connId => socket] */
    private array $subscribers = [];

    /** @var array<int, list<string>> connId => subscribed names, for cleanup disconnect */
    private array $connectionEvents = [];

    public function __construct(
        private readonly mixed $serverSocket,
        private readonly SerializerInterface $serializer,
    ) {
        $this->scope = Scope::inherit()->asNotSafely();
        $this->tasks = new TaskSet(concurrency: 64, scope: $this->scope);
    }

    public function registerLocalQueue(string $name, TransportInterface $channel): void
    {
        $this->localQueues[$name] = $channel;
    }

    public function registerRpcHandler(string $method, callable $handler): void
    {
        $this->rpcHandlers[$method] = $handler;
    }

    public function run(): void
    {
        $this->running = true;
        $this->tasks->spawn(fn() => $this->acceptLoop());

        foreach ($this->tasks as [, $error]) {
            if ($error !== null && $this->running) {
                error_log('[RpcServer] task failed: '.$error->getMessage());
            }
        }
    }

    public function stop(): void
    {
        $this->running = false;
        $this->tasks->cancel();
        @fclose($this->serverSocket);
    }

    private function acceptLoop(): void
    {
        while ($this->running) {
            $client = @stream_socket_accept($this->serverSocket, -1);
            if ($client === false) {
                continue;
            }
            $this->tasks->spawn(fn() => $this->handleConnection($client));
        }
    }

    private function handleConnection(mixed $client): void
    {
        $connId = (int) $client;
        stream_set_timeout($client, -1);

        try {
            while ($this->running) {
                $frame = FrameStream::read($client);
                if ($frame === null) {
                    break; // client close connect
                }

                match ($frame->type) {
                    FrameType::Job => $this->handleJob($frame, $client),
                    FrameType::Event => $this->handleEvent($frame),
                    FrameType::Subscribe => $this->handleSubscribe($frame, $client, $connId),
                    FrameType::RpcRequest => $this->tasks->spawn(fn() => $this->handleRpcRequest($frame, $client)),
                    default => null,
                };
            }
        } catch (\Cancellation $cancellation) {
        } catch (\Throwable $e) {
            error_log('[RpcServer] connection error: '.$e->getMessage() . mb_substr($e->getTraceAsString(), 0, 100));
        } finally {
            $this->cleanupConnection($connId);
            @fclose($client);
        }
    }

    private function handleJob(Frame $frame, mixed $client): void
    {
        $queue   = $frame->payload['queue'] ?? null;
        $channel = $queue !== null ? ($this->localQueues[$queue] ?? null) : null;

        if ($channel === null) {
            FrameStream::write($client, Frame::error(
                $frame->payload['correlationId'] ?? null,
                sprintf('Queue "%s" is not hosted in this process', $queue ?? '(none)'),
            ));

            return;
        }

        $envelope = $this->serializer->deserialize($frame->payload['envelope']);
        $channel->send($envelope);
    }

    private function handleEvent(Frame $frame): void
    {
        $name = $frame->payload['event'] ?? null;
        if ($name === null) {
            return;
        }

        $recipients = ($this->subscribers['*'] ?? [])
            + ($this->subscribers[$name] ?? []);

        $dead = [];

        foreach ($recipients as $connId => $conn) {
            try {
                FrameStream::write($conn, $frame);
            } catch (\Throwable) {
                $dead[] = [$connId, $conn];
            }
        }

        foreach ($dead as [$connId, $conn]) {
            $this->cleanupConnection($connId);
            @fclose($conn);
        }
    }

    private function handleSubscribe(Frame $frame, mixed $client, int $connId): void
    {
        $name = $frame->payload['event'] ?? null;
        if ($name === null) {
            return;
        }

        $this->subscribers[$name][$connId] = $client;
        $this->connectionEvents[$connId][] = $name;
    }

    private function handleRpcRequest(Frame $frame, mixed $client): void
    {
        $method        = $frame->payload['method'] ?? '';
        $correlationId = $frame->payload['correlationId'] ?? '';
        $handler       = $this->rpcHandlers[$method] ?? null;

        if ($handler === null) {
            FrameStream::write($client, Frame::error($correlationId, "Unknown RPC method \"{$method}\""));

            return;
        }

        try {
            $result = $handler($frame->payload['args'] ?? []);
            FrameStream::write($client, Frame::reply($correlationId, $result));
        } catch (\Throwable $e) {
            FrameStream::write($client, Frame::error($correlationId, $e->getMessage()));
        }
    }

    private function cleanupConnection(int $connId): void
    {
        foreach ($this->connectionEvents[$connId] ?? [] as $eventName) {
            unset($this->subscribers[$eventName][$connId]);
        }
        unset($this->connectionEvents[$connId]);
    }
}