<?php

namespace Thrun\Middleware;

use Thrun\Envelope\Stamp\MessageIdStamp;
use Thrun\Envelope\Stamp\RedeliveryStamp;
use Thrun\Worker\Acknowledger;
use Thrun\Worker\WorkerMiddlewareInterface;

class CatchMessageMiddleware implements WorkerMiddlewareInterface
{
    public function handle(array|object $message, Acknowledger $ack, \Closure $next): void
    {
        try {
            $next($message, $ack);
        } catch (\Throwable $e) {
            $envelope = $ack->envelope;
            $key = $envelope->routeKey ?? $envelope->type ?? gettype($envelope->message);

//            echo json_encode([
//                    'type' => 'worker_thread_handle',
//                    'job' => $key,
//                    'exception' => get_class($e),
//                    'message' => $e->getMessage(),
//                    'file' => $e->getFile(),
//                    'line' => $e->getLine(),
//                ], JSON_UNESCAPED_SLASHES) . PHP_EOL;

            $id = $envelope->last(MessageIdStamp::class)?->id ?? 0;
            $attemptStr = '';

            if ($envelope->has(RedeliveryStamp::class)) {
                /** @var RedeliveryStamp $tmp */
                $tmp = $envelope->last(RedeliveryStamp::class);

                $attemptStr = "[{$tmp->attempt}:{$tmp->retriedAt}]";
            }

            echo sprintf(
                "[WorkerThread][%s:%s]%s %s: %s (%s:%d)\n",
                $key,
                $id,
                $attemptStr,
                get_class($e),
                $e->getMessage(),
                basename($e->getFile()),
                $e->getLine()
            );

            throw $e;
        }
    }
}