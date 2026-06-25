<?php

namespace Thrun\Tests\Unit\Rpc;

use Testo\Assert;
use Testo\Test;
use Thrun\Rpc\Frame;
use Thrun\Rpc\FrameStream;
use Thrun\Rpc\FrameType;
use Thrun\Tests\AsyncTestCase;

final class FrameStreamTest extends AsyncTestCase
{
    public function roundTripsJobFrame(): void
    {
        [$a, $b] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);

        FrameStream::write($a, Frame::job('emails', '{"type":"PingMessage"}'));
        $frame = FrameStream::read($b);

        Assert::same($frame->type, FrameType::Job);
        Assert::same($frame->payload['queue'], 'emails');
        Assert::same($frame->payload['envelope'], '{"type":"PingMessage"}');

        fclose($a);
        fclose($b);
    }

    public function roundTripsEventFrame(): void
    {
        [$a, $b] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);

        FrameStream::write($a, Frame::event('order.completed', ['order_id' => 123]));
        $frame = FrameStream::read($b);

        Assert::same($frame->type, FrameType::Event);
        Assert::same($frame->payload['event'], 'order.completed');
        Assert::same($frame->payload['data']['order_id'], 123);

        fclose($a);
        fclose($b);
    }

    public function roundTripsRpcRequestAndReply(): void
    {
        [$a, $b] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);

        FrameStream::write($a, Frame::request('abc123', 'ping', ['hello' => 'world']));
        $request = FrameStream::read($b);

        Assert::same($request->type, FrameType::RpcRequest);
        Assert::same($request->payload['correlationId'], 'abc123');
        Assert::same($request->payload['method'], 'ping');

        FrameStream::write($b, Frame::reply('abc123', ['pong' => true]));
        $reply = FrameStream::read($a);

        Assert::same($reply->type, FrameType::RpcReply);
        Assert::same($reply->payload['result']['pong'], true);

        fclose($a);
        fclose($b);
    }

    public function writesMultipleFramesSequentiallyOnOneConnection(): void
    {
        [$a, $b] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);

        FrameStream::write($a, Frame::event('first', []));
        FrameStream::write($a, Frame::event('second', []));

        Assert::same(FrameStream::read($b)->payload['event'], 'first');
        Assert::same(FrameStream::read($b)->payload['event'], 'second');

        fclose($a);
        fclose($b);
    }

    public function returnsNullOnClosedConnection(): void
    {
        [$a, $b] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
        fclose($a);

        Assert::same(FrameStream::read($b), null);

        fclose($b);
    }
}