<?php

declare(strict_types=1);

namespace Thrun\Worker;

/**
 * What became of a message, as opposed to what the worker must do about it.
 *
 * Reported through Acknowledger::observe(). Without the distinction the only way
 * to keep the worker from acting on an already settled message is to call ack(),
 * which counts every such message as processed.
 *
 * String-backed because the value crosses the thread boundary.
 */
enum Outcome: string
{
    case Success = 'success';
    case Failure = 'failure';
    case Retried = 'retried';
    case Skipped = 'skipped';
}
