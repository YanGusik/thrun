<?php

declare(strict_types=1);

namespace Thrun\Tests\Fixture;

/**
 * A message that carries the outcome its handler should report, so one run can
 * mix successes, retries and failures the way a queue does.
 *
 * The value travels as a string because the handler runs in another thread.
 */
final class OutcomeMessage
{
    public function __construct(public string $outcome) {}
}
