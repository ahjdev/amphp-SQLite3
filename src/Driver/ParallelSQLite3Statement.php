<?php declare(strict_types=1);

namespace Amp\SQLite3\Driver;

use Amp\Future;
use Revolt\EventLoop;
use Amp\DeferredFuture;
use function Amp\async;
use Amp\SQLite3\Internal;
use Amp\SQLite3\SQLite3Result;
use Amp\SQLite3\SQLite3Exception;
use Amp\SQLite3\SQLite3Statement;
use Amp\Parallel\Worker\WorkerException;
use Amp\Parallel\Worker\TaskFailureThrowable;

/**
 * @psalm-import-type TFieldType from SQLite3Result
 * @template TFieldValue
 * @template TResult of SQLite3Result
 * @implements SQLite3Statement<TFieldValue>
 */
final class ParallelSQLite3Statement implements SQLite3Statement
{
    private ?string $id;

    private int $lastUsedAt;

    /** @var bool True if an operation is pending. */
    private bool $busy = false;

    private ?Future $closing = null;

    private readonly DeferredFuture $onClose;

    public function __construct(
        private readonly Internal\SQLite3Worker $worker,
        string $id,
    ) {
        $this->id = $id;
        $this->lastUsedAt = \time();
        $this->onClose = new DeferredFuture;
    }

    public function __destruct()
    {
        if ($this->id !== null && $this->worker->isRunning()) {
            $id = $this->id;
            $worker = $this->worker;
            EventLoop::queue(static fn () => $worker->execute(new Internal\SQLite3Task('closeResult', [], $id)));
        }

        if (!$this->onClose->isComplete()) {
            $this->onClose->complete();
        }
    }

    public function close(): void
    {
        if (!$this->worker->isRunning()) {
            return;
        }

        if ($this->closing) {
            $this->closing->await();
            return;
        }

        $this->writable = false;

        $this->closing = async(function (): void {
            $id = $this->id;
            $this->id = null;
            $this->worker->execute(new Internal\SQLite3Task('closeStmt', [], $id));
        });

        try {
            $this->closing->await();
        } finally {
            $this->onClose->complete();
        }
    }

    public function isClosed(): bool
    {
        return $this->closing !== null;
    }

    public function onClose(\Closure $onClose): void
    {
        $this->onClose->getFuture()->finally($onClose);
    }

    public function execute(array $params = []): ParallelSQLite3Result
    {
        if ($this->id === null) {
            throw new SQLite3Exception("The SQLite3Stmt has been closed");
        }

        $this->lastUsedAt = \time();

        // if ($this->busy) {
        //     throw new PendingOperationError;
        // }

        // if (!$this->writable) {
        //     throw new ClosedException("The file is no longer writable");
        // }

        // ++$this->pendingWrites;
        // $this->busy = true;

        try {
            return $this->worker->execute(new Internal\SQLite3Task('execute', $params, $this->id));
        } catch (TaskFailureThrowable $exception) {
            throw new SQLite3Exception("Executing statement from the SQLite3Stmt failed", 0, $exception);
        } catch (WorkerException $exception) {
            throw new SQLite3Exception("Sending the task to the worker failed", 0, $exception);
        } finally {
            // if (--$this->pendingWrites === 0) {
            //     $this->busy = false;
            // }
        }
    }

    public function getQuery(): string
    {
        if ($this->id === null) {
            throw new SQLite3Exception("The SQLite3Result has been closed");
        }

        // if ($this->busy) {
        //     throw new PendingOperationError;
        // }

        // if (!$this->writable) {
        //     throw new ClosedException("The file is no longer writable");
        // }

        // ++$this->pendingWrites;
        // $this->busy = true;

        try {
            return $this->worker->execute(new Internal\SQLite3Task('getQuery', [], $this->id));
        } catch (TaskFailureThrowable $exception) {
            throw new SQLite3Exception("Getting query from the SQLite3Stmt failed", 0, $exception);
        } catch (WorkerException $exception) {
            throw new SQLite3Exception("Sending the task to the worker failed", 0, $exception);
        } finally {
            // if (--$this->pendingWrites === 0) {
            //     $this->busy = false;
            // }
        }
    }

    public function getLastUsedAt(): int
    {
        return $this->lastUsedAt;
    }
}
