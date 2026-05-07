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

        while (true) {
            /** @var Worker $worker */
            $worker = ($this->workerFactory)();

            $workerCoro = \Async\spawn(fn() => $worker->run());

            $sigCoro = \Async\spawn(function () use ($worker, $workerCoro): void {
                try {
                    $signal = \Async\await(\Async\await_any_or_fail([
                        \Async\signal(\Async\Signal::SIGINT),
                        \Async\signal(\Async\Signal::SIGTERM),
                    ]));
                    
                    printf("\n[Supervisor] Signal %s received. Stopping worker...\n", $signal->name);
                    
                    $worker->stop();
                    $workerCoro->cancel();
                } catch (\Throwable $e) {
                    // ignore
                }
            });

            try {
                \Async\await($workerCoro);
                $sigCoro->cancel();

                return;
            } catch (\Async\Cancellation) {
                $sigCoro->cancel();

                return;
            } catch (\Throwable $e) {
                $sigCoro->cancel();

                $now     = time();
                $crashes = array_values(array_filter(
                    $crashes,
                    static fn(int $t): bool => $now - $t < $this->options->restartWindow,
                ));
                $crashes[] = $now;

                if (count($crashes) >= $this->options->maxCrashes) {
                    throw $e;
                }

                \Async\delay((int) ceil($currentBackoff * 1000));
                $currentBackoff *= 2.0;
            }
        }
    }
}
