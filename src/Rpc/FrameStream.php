<?php

declare(strict_types=1);

namespace Thrun\Rpc;

/**
 * Length-prefixed framing поверх сырого сокета.
 * Layout: [4 bytes BE length][1 byte frameType][N bytes JSON payload]
 */
final class FrameStream
{
    private const int HEADER_LENGTH = 5;

    public static function read(mixed $stream): ?Frame
    {
        $header = self::readExact($stream, self::HEADER_LENGTH);
        if ($header === null) {
            return null; // connection closed
        }

        $length = unpack('N', substr($header, 0, 4))[1];
        $type   = FrameType::from(ord($header[4]));
        $body   = $length > 0 ? self::readExact($stream, $length) : '';

        if ($body === null) {
            return null;
        }

        /** @var array $payload */
        $payload = json_decode($body, true, 512, JSON_THROW_ON_ERROR);

        return new Frame($type, $payload);
    }

    public static function write(mixed $stream, Frame $frame): void
    {
        $body   = json_encode($frame->payload, JSON_THROW_ON_ERROR);
        $header = pack('N', strlen($body)).chr($frame->type->value);

        self::writeExact($stream, $header.$body);
    }

    private static function readExact(mixed $stream, int $length): ?string
    {
        $buffer = '';
        while (strlen($buffer) < $length) {
            $chunk = fread($stream, $length - strlen($buffer));
            if ($chunk === false || $chunk === '') {
                return null;
            }
            $buffer .= $chunk;
        }

        return $buffer;
    }

    private static function writeExact(mixed $stream, string $data): void
    {
        $total   = strlen($data);
        $written = 0;
        while ($written < $total) {
            $n = fwrite($stream, substr($data, $written));
            if ($n === false || $n === 0) {
                throw new \RuntimeException('Failed to write frame to stream');
            }
            $written += $n;
        }
    }
}