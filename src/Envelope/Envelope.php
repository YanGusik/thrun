<?php

declare(strict_types=1);

namespace Thrun\Envelope;

use Thrun\Contract\StampInterface;
use Thrun\Envelope\Stamp\JobIdStamp;

final class Envelope
{
    /** @var array<class-string<StampInterface>, list<StampInterface>> */
    private array $stamps = [];

    public function __construct(
        public readonly object|array $message,
        public readonly ?string $type = null,
        public readonly ?string $routeKey = null,
        array $stamps = [],
    ) {
        foreach ($stamps as $stamp) {
            $this->stamps[$stamp::class][] = $stamp;
        }
    }

    public static function wrap(object|array $message, StampInterface ...$stamps): self
    {
        $type = is_object($message) ? $message::class : 'array';

        foreach ($stamps as $stamp) {
            if ($stamp instanceof JobIdStamp) {
                return new self($message, type: $type, stamps: $stamps);
            }
        }

        return new self($message, type: $type, stamps: [...$stamps, new JobIdStamp()]);
    }

    public function with(StampInterface ...$stamps): self
    {
        $clone = clone $this;

        foreach ($stamps as $stamp) {
            $clone->stamps[$stamp::class][] = $stamp;
        }

        return $clone;
    }

    public function withoutAll(string $stampClass): self
    {
        $clone = clone $this;
        unset($clone->stamps[$stampClass]);

        return $clone;
    }

    /**
     * Returns the last stamp of the given type, or null if not present.
     *
     * @template T of StampInterface
     * @param class-string<T> $stampClass
     * @return T|null
     */
    public function last(string $stampClass): ?StampInterface
    {
        $list = $this->stamps[$stampClass] ?? [];

        return $list !== [] ? $list[array_key_last($list)] : null;
    }

    /**
     * Returns all stamps of the given type.
     *
     * @template T of StampInterface
     * @param class-string<T> $stampClass
     * @return list<T>
     */
    public function all(string $stampClass): array
    {
        return $this->stamps[$stampClass] ?? [];
    }

    public function has(string $stampClass): bool
    {
        return isset($this->stamps[$stampClass]);
    }

    /**
     * @return list<StampInterface>
     */
    public function allStamps(): array
    {
        $result = [];
        foreach ($this->stamps as $list) {
            foreach ($list as $stamp) {
                $result[] = $stamp;
            }
        }
        return $result;
    }
}
