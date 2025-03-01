<?php declare(strict_types=1);

namespace Amp\SQLite3\Driver;

use Amp\Future;
use Revolt\EventLoop;
use Amp\DeferredFuture;
use function Amp\async;
use Amp\SQLite3\Internal;
use Amp\SQLite3\SQLite3Config;
use Amp\SQLite3\SQLite3Driver;
use Amp\Parallel\Worker\Worker;
use Amp\Sql\SqlTransientResource;
use Amp\SQLite3\SQLite3Exception;
use Amp\SQLite3\SQLite3QueryError;
use Amp\Parallel\Context\StatusError;
use Amp\Parallel\Worker\WorkerException;
use Amp\Parallel\Worker\ContextWorkerPool;
use Amp\Parallel\Worker\LimitedWorkerPool;
use Amp\SQLite3\SQLite3ConnectionException;
use Amp\Parallel\Worker\TaskFailureThrowable;

/** @internal */
final class ParallelSQLite3Driver implements SqlTransientResource, SQLite3Driver
{
    public const DEFAULT_WORKER_LIMIT = 8;

    /** @var \WeakMap<Worker, int> */
    private \WeakMap $workerStorage;

    /** @var Future<Worker>|null Pending worker request */
    private ?Future $pendingWorker = null;

    /** @var \SplQueue<DeferredFuture> */
    private readonly \SplQueue $deferreds;

    /** @var \SplQueue<\Closure():void> */
    private readonly \SplQueue $onReady;

    private readonly DeferredFuture $onClose;

    private readonly LimitedWorkerPool $pool;

    private int $lastUsedAt;

    public function __construct(private readonly SQLite3Config $config)
    {
        $this->lastUsedAt = \time();
        /** @var \WeakMap<Worker, int> For Psalm. */
        $this->workerStorage = new \WeakMap();
        $this->deferreds = new \SplQueue;
        $this->onReady = new \SplQueue;
        $this->onClose = new DeferredFuture;
        $this->pool    = new ContextWorkerPool(self::DEFAULT_WORKER_LIMIT, new Internal\SQLite3WorkerFactory($config));
    }

    private function selectWorker(): Worker
    {
        $this->pendingWorker?->await(); // Wait for any currently pending request for a worker.

        if ($this->workerStorage->count() < $this->pool->getWorkerLimit()) {
            $this->pendingWorker = async($this->pool->getWorker(...));
            $worker = $this->pendingWorker->await();
            $this->pendingWorker = null;

            $this->workerStorage[$worker] = 1;

            return $worker;
        }

        $max = \PHP_INT_MAX;
        foreach ($this->workerStorage as $storedWorker => $count) {
            if ($count <= $max) {
                $worker = $storedWorker;
                $max = $count;
            }
        }

        \assert(isset($worker) && $worker instanceof Worker);

        if (!$worker->isRunning()) {
            unset($this->workerStorage[$worker]);
            return $this->selectWorker();
        }

        $this->workerStorage[$worker] += 1;

        return $worker;
    }

    public function close(): void
    {
        if ($this->onClose->isComplete()) {
            return;
        }

        try {
            $this->onClose->complete();
            $this->pool->shutdown();
        } catch (StatusError $e) {
            throw new SQLite3ConnectionException($e->getMessage(), $e->getCode(), $e);
        }
    }

    public function isClosed(): bool
    {
        return !$this->pool->isRunning();
    }

    public function onClose(\Closure $onClose): void
    {
        $this->onClose->getFuture()->finally($onClose);
    }

    // private function handleError(DeferredFuture $deferred, SQLite3ChannelException $error): void
    // {
    //     $query   = $error->query;
    //     $errcode = $error->code;
    //     $message = $error->message;
    //     // SQLite3 exception
    //     $error = new SQLite3Exception($message, $errcode);
    //     // SQLite3 query exception error
    //     if (
    //         \str_ends_with($message, 'incomplete input') ||
    //         \str_ends_with($message, 'syntax error')
    //     ) {
    //         $error = new SQLite3QueryError($message, $query);
    //     }
    //     // Throw exception
    //     $deferred->error($error);
    // }

    public function getConfig(): SQLite3Config
    {
        return $this->config;
    }

    public function getLastUsedAt(): int
    {
        return $this->lastUsedAt;
    }


    public function query(string $query): ParallelSQLite3Result|ParallelSQLite3CommandResult
    {
        $worker = $this->selectWorker();

        $workerStorage = $this->workerStorage;
        $worker = new Internal\SQLite3Worker($worker, static function (Worker $worker) use ($workerStorage): void {
            if (!isset($workerStorage[$worker])) {
                return;
            }

            if (($workerStorage[$worker] -= 1) === 0 || !$worker->isRunning()) {
                unset($workerStorage[$worker]);
            }
        });

        try {
            [$id, $lastInsertId, $affectedRows, $columnCount] = $worker->execute(new Internal\SQLite3Task("query", [$query]));
        } catch (TaskFailureThrowable $exception) {
            throw new SQLite3QueryError("Could not open SQLite3Result", previous: $exception);
        } catch (WorkerException $exception) {
            throw new SQLite3Exception("Could not send query request to worker", previous: $exception);
        }

        if ($columnCount) {
            return new ParallelSQLite3Result($worker, $id, $lastInsertId, $affectedRows, $columnCount);
        }
        return new ParallelSQLite3CommandResult($affectedRows, $lastInsertId);
    }

    public function prepare(string $query): ParallelSQLite3Statement
    {
        $worker = $this->selectWorker();

        $workerStorage = $this->workerStorage;
        $worker = new Internal\SQLite3Worker($worker, static function (Worker $worker) use ($workerStorage): void {
            if (!isset($workerStorage[$worker])) {
                return;
            }

            if (($workerStorage[$worker] -= 1) === 0 || !$worker->isRunning()) {
                unset($workerStorage[$worker]);
            }
        });

        try {
            [$id] = $worker->execute(new Internal\SQLite3Task("prepare", [$query]));
        } catch (TaskFailureThrowable $exception) {
            throw new SQLite3QueryError("Could not open SQLite3Stmt", previous: $exception);
        } catch (WorkerException $exception) {
            throw new SQLite3Exception("Could not send prepare request to worker", previous: $exception);
        }
        return new ParallelSQLite3Statement($worker, $id);
    }

    public function execute(string $query, array $params = []): \Amp\SQLite3\SQLite3Result
    {
        return $this->prepare($query)->execute($params);
    }

    public function __destruct()
    {
        EventLoop::queue($this->close(...));
    }
}
