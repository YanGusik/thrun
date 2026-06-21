<?php

namespace Thrun\Tests\Fixture;

final class ThreadLocalPublisher
{
    private static mixed $connection = null;

    public static function get(string $socketPath): mixed
    {
        if (self::$connection === null) {
            echo "create connect\n";
            self::$connection = stream_socket_client("unix://{$socketPath}");
        }

        return self::$connection;
    }
}