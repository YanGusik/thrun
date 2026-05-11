<?php

declare(strict_types=1);

namespace Thrun\Serialization;

use Thrun\Contract\MessageTypeResolverInterface;

final class ClassMapMessageTypeResolver implements MessageTypeResolverInterface
{
    /** @var array<string, class-string> */
    private array $map = [];

    /**
     * @param array<string, class-string> $map alias => class-string
     */
    public function __construct(array $map = [])
    {
        $this->map = $map;
    }

    public function register(string $alias, string $class): void
    {
        $this->map[$alias] = $class;
    }

    public function resolve(string $type): string
    {
        return $this->map[$type] ?? $type;
    }
}
