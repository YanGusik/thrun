<?php

declare(strict_types=1);

namespace Thrun\Contract;

interface MessageTypeResolverInterface
{
    /**
     * Resolve a message type alias (or FQN) to a concrete class-string.
     *
     * @return class-string
     */
    public function resolve(string $type): string;
}
