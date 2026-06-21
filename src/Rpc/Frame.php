<?php

declare(strict_types=1);

namespace Thrun\Rpc;

final readonly class Frame
{
    public function __construct(
        public FrameType $type,
        public array $payload,
    ) {}

    public static function job(string $queue, string $serializedEnvelope): self
    {
        return new self(FrameType::Job, ['queue' => $queue, 'envelope' => $serializedEnvelope]);
    }

    public static function event(string $name, array $data): self
    {
        return new self(FrameType::Event, ['event' => $name, 'data' => $data]);
    }

    public static function subscribe(string $name): self
    {
        return new self(FrameType::Subscribe, ['event' => $name]);
    }

    public static function request(string $correlationId, string $method, array $args): self
    {
        return new self(FrameType::RpcRequest, [
            'correlationId' => $correlationId,
            'method'        => $method,
            'args'          => $args,
        ]);
    }

    public static function reply(string $correlationId, mixed $result): self
    {
        return new self(FrameType::RpcReply, ['correlationId' => $correlationId, 'result' => $result]);
    }

    public static function error(?string $correlationId, string $message): self
    {
        return new self(FrameType::Error, ['correlationId' => $correlationId, 'message' => $message]);
    }
}