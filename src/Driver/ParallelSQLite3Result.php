<?php declare(strict_types=1);

namespace Amp\SQLite3\Driver;

use Amp\Future;
use Revolt\EventLoop;
use Amp\DeferredFuture;
use function Amp\async;
use Amp\SQLite3\Internal;
use Amp\SQLite3\SQLite3Result;
use Amp\SQLite3\SQLite3Exception;
use Amp\Parallel\Worker\WorkerException;
use Amp\Parallel\Worker\TaskFailureThrowable;

/**
 * @psalm-import-type TFieldType from SQLite3Result
 * @template TFieldValue
 * @template TResult of SQLite3Result
 * @implements SQLite3Result<TFieldValue>
 * @implements \IteratorAggregate<int, array<string, TFieldValue>>
 */
final class ParallelSQLite3Result implements SQLite3Result, \IteratorAggregate
{
    private ?int $id;

    /** @var bool True if an operation is pending. */
    private bool $busy = false;

    private ?Future $closing = null;

    private readonly DeferredFuture $onClose;

    public function __construct(
        private readonly Internal\SQLite3Worker $worker,
        int $id,
        private readonly ?int $lastInsertId,
        private readonly ?int $affectedRows,
        private int $columnCount,
    ) {
        $this->id = $id;
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
            $this->worker->execute(new Internal\SQLite3Task('closeResult', [], $id));
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

    public function columnName(int $column): string
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
            return $this->worker->execute(new Internal\SQLite3Task('columnName', [$column], $this->id));
        } catch (TaskFailureThrowable $exception) {
            throw new SQLite3Exception("Reading columnName from the SQLite3Result failed", 0, $exception);
        } catch (WorkerException $exception) {
            throw new SQLite3Exception("Sending the task to the worker failed", 0, $exception);
        } finally {
            // if (--$this->pendingWrites === 0) {
            //     $this->busy = false;
            // }
        }
    }

    public function columnType(int $column): int
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
            return $this->worker->execute(new Internal\SQLite3Task('columnType', [$column], $this->id));
        } catch (TaskFailureThrowable $exception) {
            throw new SQLite3Exception("Reading columnType from the SQLite3Result failed", 0, $exception);
        } catch (WorkerException $exception) {
            throw new SQLite3Exception("Sending the task to the worker failed", 0, $exception);
        } finally {
            // if (--$this->pendingWrites === 0) {
            //     $this->busy = false;
            // }
        }
    }

    public function getNextResult(): ?self
    {
        return null;
    }

    public function getLastInsertId(): ?int
    {
        return $this->lastInsertId;
    }

    public function fetchRow(): ?array
    {
        return $this->worker->execute(new Internal\SQLite3Task('fetchRow', [], $this->id));
    }

    public function getRowCount(): ?int
    {
        return $this->affectedRows;
    }

    public function getColumnCount(): ?int
    {
        return $this->columnCount;
    }

    public function getIterator(): \Traversable
    {
        while (!$fetch = $this->fetchRow()) {
            yield $fetch;
        }
    }
}
