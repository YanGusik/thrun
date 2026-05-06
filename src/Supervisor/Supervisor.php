<?php

declare(strict_types=1);

namespace Thrun\Supervisor;

use Closure;
use Thrun\Worker\Worker;

final class Supervisor
{
    public function __construct(
        private readonly Closure $workerFactory,
        private readonly SupervisorOptions $options = new SupervisorOptions(),
    ) {
    }

    public function run(): void
    {
        $crashes        = [];
        $currentBackoff = $this->options->restartBackoff;

        pcntl_async_signals(true);

        while (true) {
            /** @var Worker $worker */
            $worker = ($this->workerFactory)();

            pcntl_signal(SIGTERM, static function () use ($worker): void {
                $worker->stop();
            });
            pcntl_signal(SIGINT, static function () use ($worker): void {
                $worker->stop();
            });

            try {
                $worker->run();

                return;
            } catch (\Throwable $e) {
                $now       = time();
                $crashes   = array_values(array_filter(
                    $crashes,
                    static fn(int $t): bool => $now - $t < $this->options->restartWindow,
                ));
                $crashes[] = $now;

                if (count($crashes) >= $this->options->maxCrashes) {
                    throw $e;
                }

                sleep((int) ceil($currentBackoff));
                $currentBackoff *= 2.0;
            }
        }
    }
}
