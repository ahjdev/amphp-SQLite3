<?php declare(strict_types=1);

namespace Amp\SQLite3\Driver;

use Amp\File\FilesystemDriver;
use Amp\File\FilesystemException;
use Amp\File\Internal;
use Amp\Future;
use Amp\Parallel\Worker\ContextWorkerPool;
use Amp\Parallel\Worker\DelegatingWorkerPool;
use Amp\Parallel\Worker\LimitedWorkerPool;
use Amp\Parallel\Worker\TaskFailureThrowable;
use Amp\Parallel\Worker\Worker;
use Amp\Parallel\Worker\WorkerException;
use Amp\Parallel\Worker\WorkerPool;
use Amp\SQLite3\SQLite3Driver;
use function Amp\async;

// private readonly int $id,
// private readonly ?int $lastInsertId,
// private readonly ?int $affectedRows,

final class ParallelSQLite3Driver implements SQLite3Driver
{
    private ?int $id;

    private int $position;

    private int $size;

    /** @var bool True if an operation is pending. */
    private bool $busy = false;

    /** @var int Number of pending write operations. */
    private int $pendingWrites = 0;

    private bool $writable;

    private ?Future $closing = null;

    private readonly DeferredFuture $onClose;

    private ?LockType $lockType = null;

    public function __construct(
        private readonly Internal\FileWorker $worker,
        int $id,
        private readonly string $path,
        int $size,
        private readonly string $mode,
    ) {
        $this->id = $id;
        $this->size = $size;
        $this->writable = $this->mode[0] !== 'r';
        $this->position = $this->mode[0] === 'a' ? $this->size : 0;

        $this->onClose = new DeferredFuture;
    }

    public function __destruct()
    {
        if ($this->id !== null && $this->worker->isRunning()) {
            $id = $this->id;
            $worker = $this->worker;
            EventLoop::queue(static fn () => $worker->execute(new Internal\FileTask('fclose', [], $id)));
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
            $this->worker->execute(new Internal\FileTask('fclose', [], $id));
        });

        try {
            $this->closing->await();
        } finally {
            $this->onClose->complete();
            $this->lockType = null;
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

    public function truncate(int $size): void
    {
        if ($this->id === null) {
            throw new ClosedException("The file has been closed");
        }

        if ($this->busy) {
            throw new PendingOperationError;
        }

        if (!$this->writable) {
            throw new ClosedException("The file is no longer writable");
        }

        ++$this->pendingWrites;
        $this->busy = true;

        try {
            $this->worker->execute(new Internal\FileTask('ftruncate', [$size], $this->id));
            $this->size = $size;
        } catch (TaskFailureException $exception) {
            throw new StreamException("Reading from the file failed", 0, $exception);
        } catch (WorkerException $exception) {
            throw new StreamException("Sending the task to the worker failed", 0, $exception);
        } finally {
            if (--$this->pendingWrites === 0) {
                $this->busy = false;
            }
        }
    }

    public function eof(): bool
    {
        return $this->pendingWrites === 0 && $this->size <= $this->position;
    }

    public function lock(LockType $type, ?Cancellation $cancellation = null): void
    {
        $this->flock('lock', $type, $cancellation);
        $this->lockType = $type;
    }

    public function tryLock(LockType $type): bool
    {
        $locked = $this->flock('try-lock', $type);
        if ($locked) {
            $this->lockType = $type;
        }

        return $locked;
    }

    public function unlock(): void
    {
        $this->flock('unlock');
        $this->lockType = null;
    }

    public function getLockType(): ?LockType
    {
        return $this->lockType;
    }

    private function flock(string $action, ?LockType $type = null, ?Cancellation $cancellation = null): bool
    {
        if ($this->id === null) {
            throw new ClosedException("The file has been closed");
        }

        $this->busy = true;

        try {
            return $this->worker->execute(new Internal\FileTask('flock', [$type, $action], $this->id), $cancellation);
        } catch (TaskFailureException $exception) {
            throw new StreamException("Attempting to lock the file failed", 0, $exception);
        } catch (WorkerException $exception) {
            throw new StreamException("Sending the task to the worker failed", 0, $exception);
        } finally {
            $this->busy = false;
        }
    }

    public function read(?Cancellation $cancellation = null, int $length = self::DEFAULT_READ_LENGTH): ?string
    {
        if ($this->id === null) {
            throw new ClosedException("The file has been closed");
        }

        if ($this->busy) {
            throw new PendingOperationError;
        }

        $this->busy = true;

        try {
            $data = $this->worker->execute(new Internal\FileTask('fread', [$length], $this->id), $cancellation);

            if ($data !== null) {
                $this->position += \strlen($data);
            }
        } catch (TaskFailureException $exception) {
            throw new StreamException("Reading from the file failed", 0, $exception);
        } catch (WorkerException $exception) {
            throw new StreamException("Sending the task to the worker failed", 0, $exception);
        } finally {
            $this->busy = false;
        }

        return $data;
    }

    public function write(string $bytes): void
    {
        if ($this->id === null) {
            throw new ClosedException("The file has been closed");
        }

        if ($this->busy && $this->pendingWrites === 0) {
            throw new PendingOperationError;
        }

        if (!$this->writable) {
            throw new ClosedException("The file is no longer writable");
        }

        ++$this->pendingWrites;
        $this->busy = true;

        try {
            $this->worker->execute(new Internal\FileTask('fwrite', [$bytes], $this->id));
            $this->position += \strlen($bytes);
            $this->size = \max($this->position, $this->size);
        } catch (TaskFailureException $exception) {
            throw new StreamException("Writing to the file failed", 0, $exception);
        } catch (WorkerException $exception) {
            throw new StreamException("Sending the task to the worker failed", 0, $exception);
        } finally {
            if (--$this->pendingWrites === 0) {
                $this->busy = false;
            }
        }
    }

    public function end(): void
    {
        $this->writable = false;
        $this->close();
    }

    public function seek(int $position, Whence $whence = Whence::Start): int
    {
        if ($this->id === null) {
            throw new ClosedException("The file has been closed");
        }

        if ($this->busy) {
            throw new PendingOperationError;
        }

        switch ($whence) {
            case Whence::Start:
            case Whence::Current:
            case Whence::End:
                try {
                    $this->position = $this->worker->execute(
                        new Internal\FileTask('fseek', [$position, $whence], $this->id)
                    );

                    $this->size = \max($this->position, $this->size);

                    return $this->position;
                } catch (TaskFailureException $exception) {
                    throw new StreamException('Seeking in the file failed.', 0, $exception);
                } catch (WorkerException $exception) {
                    throw new StreamException("Sending the task to the worker failed", 0, $exception);
                }

            default:
                throw new \Error('Invalid whence value. Use Start, Current, or End.');
        }
    }

}
