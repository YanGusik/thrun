<?php

declare(strict_types=1);

namespace Thrun\Supervisor;

use Cancellation;
use Closure;
use Async\Coroutine;
use Async\Signal;
use Throwable;
use Thrun\Exception\AllThreadDiedException;
use Thrun\Worker\Worker;
use function Async\await;
use function Async\await_any_or_fail;
use function Async\signal;
use function Async\spawn;

final class Supervisor
{
    private Coroutine $workerCoro;
    private Coroutine $sigCoro;
    private Worker $worker;

    public function __construct(
        private readonly Closure $workerFactory,
        private readonly SupervisorOptions $options = new SupervisorOptions(),
    ) {
    }

    public function stop(): void
    {
        $this->workerCoro->cancel();
        $this->sigCoro->cancel();
    }

    public function run(): void
    {
        $crashes        = [];
        $currentBackoff = $this->options->restartBackoff;

        while (true) {
            echo "main\n";
            /** @var Worker $worker */
            $this->worker = ($this->workerFactory)();

            $this->sigCoro = spawn(function (): void {
                $signal = await_any_or_fail([
                    signal(Signal::SIGINT),
                    signal(Signal::SIGTERM),
                ]);
                printf("\n[Supervisor] Signal %s received. Stopping worker...\n", $signal->name);

                $this->worker->stop();
                $this->sigCoro->cancel();
            });

            try {
                $this->workerCoro = spawn(fn() => $this->worker->run());
                await($this->workerCoro);

                return;
            }
            catch (\Cancellation) {}
            catch (Throwable $e) {
                error_log('[Thrun Supervisor] Caught '.get_class($e).': '.$e->getMessage() .': '.$e->getTraceAsString());

                $now       = time();
                $window    = $this->options->restartWindow;
                $crashes   = array_values(array_filter(
                    $crashes,
                    static fn(int $t): bool => $now - $t < $window,
                ));
                $crashes[] = $now;

                if (count($crashes) >= $this->options->maxCrashes) {
                    throw $e;
                }

                \Async\delay((int) ceil($currentBackoff * 1000));
                $currentBackoff *= 2.0;
            } finally {
                $this->sigCoro->cancel();
            }
        }
    }
}
