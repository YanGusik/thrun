<?php

declare(strict_types=1);

namespace Thrun\Rpc;

enum FrameType: int
{
    case Job        = 0x01;
    case Event      = 0x02;
    case Subscribe  = 0x03;
    case RpcRequest = 0x04;
    case RpcReply   = 0x05;
    case Error      = 0x06;
}