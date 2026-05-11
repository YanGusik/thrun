<?php

declare(strict_types=1);

namespace Thrun\Contract;

use Thrun\Envelope\Envelope;

interface SerializerInterface
{
    /**
     * Serialize an Envelope to a string (e.g. JSON).
     */
    public function serialize(Envelope $envelope): string;

    /**
     * Deserialize a string back to an Envelope.
     */
    public function deserialize(string $data): Envelope;
}
