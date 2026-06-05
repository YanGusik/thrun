<?php

declare(strict_types=1);

namespace Thrun\Contract;

/**
 * I think it will be useful when there are complex stamps that are difficult to normalize
 */
interface NormalizableStampInterface extends StampInterface
{
    /** @return array<string, mixed> */
    public function normalize(): array;

    public static function denormalize(array $data): self;
}
