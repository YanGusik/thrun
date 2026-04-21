<?php

declare(strict_types=1);

namespace Thrun\Contract;

use Thrun\Envelope\Envelope;

interface SenderInterface
{
    public function send(Envelope $envelope): void;
}
