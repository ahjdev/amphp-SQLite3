<?php declare(strict_types=1);

/**
 * This file is part of Reymon.
 * Reymon is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 * Reymon is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * @author    AhJ <AmirHosseinJafari8228@gmail.com>
 * @copyright 2023-2024 AhJ <AmirHosseinJafari8228@gmail.com>
 * @license   https://choosealicense.com/licenses/gpl-3.0/ GPLv3
 */

namespace Amp\SQLite3\Internal;

use Amp\DeferredFuture;
use Amp\Sql\SqlTransactionIsolation;
use Amp\SQLite3\SQLite3Exception;
use Amp\SQLite3\SQLite3Result;
use Amp\SQLite3\SQLite3Statement;
use Amp\SQLite3\SQLite3Transaction;
use Amp\SQLite3\SQLite3TransactionError;
use Revolt\EventLoop;

/**
 * @internal
 * @template TResult of SQLite3Result
 * @template TStatement of SQLite3Statement<TResult>
 * @template TTransaction of SQLite3Transaction
 * @template TNestedExecutor of SQLite3NestableExecutor<TResult, TStatement>
 *
 * @implements SQLite3Transaction<TResult, TStatement, TTransaction>
 */
final class SQLite3NestedTransaction implements SQLite3Transaction
{
    /** @var \Closure():void */
    private readonly \Closure $release;

    private int $refCount = 1;

    private bool $active = true;

    private readonly DeferredFuture $onCommit;
    private readonly DeferredFuture $onRollback;
    private readonly DeferredFuture $onClose;

    private ?DeferredFuture $busy = null;

    private int $nextId = 1;

    /**
     * Creates a Result of the appropriate type using the Result object returned by the Link object and the
     * given release callable.
     *
     * @param TResult $result
     * @param \Closure():void $release
     *
     * @return TResult
     */
    protected function createResult(SQLite3Result $result, \Closure $release): SQLite3Result
    {
        return new SQLite3PooledResult($result, $release);
    }

    /**
     * @param TTransaction $transaction
     * @param TNestedExecutor $executor
     * @param non-empty-string $identifier
     * @param \Closure():void $release
     *
     * @return TTransaction
     */
    protected function createNestedTransaction(
        SQLite3Transaction $transaction,
        SQLite3NestableExecutor $executor,
        string $identifier,
        \Closure $release,
    ): SQLite3Transaction {
        return new self($transaction, $executor, $identifier, $release);
    }

    protected function createStatement(
        SQLite3Statement $statement,
        \Closure $release,
        ?\Closure $awaitBusyResource = null,
    ): SQLite3Statement {
        return new SQLite3PooledStatement($statement, $release, $awaitBusyResource);
    }

    protected function getTransaction(): SQLite3Transaction
    {
        return $this->transaction;
    }

    /**
     * @param TTransaction $transaction Transaction object created by connection.
     * @param TNestedExecutor $executor
     * @param non-empty-string $identifier
     * @param \Closure():void $release Callable to be invoked when the transaction completes or is destroyed.
     */
    public function __construct(
        private readonly SQLite3Transaction $transaction,
        private readonly SQLite3NestableExecutor $executor,
        private readonly string $identifier,
        \Closure $release,
    ) {
        $this->onCommit   = new DeferredFuture();
        $this->onRollback = new DeferredFuture();
        $this->onClose    = new DeferredFuture();

        $busy = &$this->busy;
        $refCount = &$this->refCount;
        $this->release = static function () use (&$busy, &$refCount, $release): void {
            $busy?->complete();
            $busy = null;

            if (--$refCount === 0) {
                $release();
            }
        };

        $this->onClose($this->release);

        if (!$this->transaction->isActive()) {
            $this->active = false;
            $this->onClose->complete();
        }
    }

    public function __destruct()
    {
        if ($this->onClose->isComplete()) {
            return;
        }

        $this->onClose->complete();

        if ($this->executor->isClosed()) {
            return;
        }

        $busy        = &$this->busy;
        $transaction = $this->transaction;
        $executor    = $this->executor;
        $identifier  = $this->identifier;
        $onRollback  = $this->onRollback;
        $onClose     = $this->onClose;
        EventLoop::queue(static function () use (
            &$busy,
            $transaction,
            $executor,
            $identifier,
            $onRollback,
            $onClose,
        ): void {
            try {
                while ($busy) {
                    $busy->getFuture()->await();
                }

                if ($transaction->isActive() && !$executor->isClosed()) {
                    $executor->rollbackTo($identifier);
                }
            } catch (SQLite3Exception) {
                // Ignore failure if connection closes during query.
            } finally {
                $transaction->onRollback(static fn () => $onRollback->isComplete() || $onRollback->complete());
                $onClose->complete();
            }
        });
    }

    public function query(string $sql): SQLite3Result
    {
        $this->awaitPendingNestedTransaction();
        ++$this->refCount;

        try {
            $result = $this->executor->query($sql);
            return $this->createResult($result, $this->release);
        } catch (\Throwable $exception) {
            EventLoop::queue($this->release);
            throw $exception;
        }
    }

    public function prepare(string $sql): SQLite3Statement
    {
        $this->awaitPendingNestedTransaction();

        return $this->executor->prepare($sql);
    }

    public function execute(string $sql, array $params = []): SQLite3Result
    {
        $this->awaitPendingNestedTransaction();
        ++$this->refCount;

        try {
            $result = $this->executor->execute($sql, $params);
            return $this->createResult($result, $this->release);
        } catch (\Throwable $exception) {
            EventLoop::queue($this->release);
            throw $exception;
        }
    }

    public function beginTransaction(): SQLite3Transaction
    {
        $this->awaitPendingNestedTransaction();
        ++$this->refCount;
        $this->busy = new DeferredFuture();

        $identifier = $this->identifier . '-' . $this->nextId++;

        try {
            $this->executor->createSavepoint($identifier);
            return $this->createNestedTransaction($this->transaction, $this->executor, $identifier, $this->release);
        } catch (\Throwable $exception) {
            EventLoop::queue($this->release);
            throw $exception;
        }
    }

    public function isClosed(): bool
    {
        return $this->onClose->isComplete();
    }

    /**
     * Rolls back the transaction if it has not been committed.
     */
    public function close(): void
    {
        if (!$this->active) {
            return;
        }

        $this->rollback();
    }

    public function onClose(\Closure $onClose): void
    {
        $this->onClose->getFuture()->finally($onClose);
    }

    public function isActive(): bool
    {
        return $this->active && $this->transaction->isActive();
    }

    public function commit(): void
    {
        $this->active = false;
        $this->awaitPendingNestedTransaction();

        try {
            $this->executor->releaseSavepoint($this->identifier);
        } finally {
            $onCommit = $this->onCommit;
            $this->transaction->onCommit(static fn () => $onCommit->isComplete() || $onCommit->complete());

            $onRollback = $this->onRollback;
            $this->transaction->onRollback(static fn () => $onRollback->isComplete() || $onRollback->complete());

            $this->onClose->complete();
        }
    }

    public function rollback(): void
    {
        $this->active = false;
        $this->awaitPendingNestedTransaction();

        try {
            $this->executor->rollbackTo($this->identifier);
        } finally {
            $this->onRollback->complete();
            $this->onClose->complete();
        }
    }

    public function onCommit(\Closure $onCommit): void
    {
        $this->onCommit->getFuture()->finally($onCommit);
    }

    public function onRollback(\Closure $onRollback): void
    {
        $this->onRollback->getFuture()->finally($onRollback);
    }

    public function getSavepointIdentifier(): string
    {
        return $this->identifier;
    }

    public function getIsolation(): SqlTransactionIsolation
    {
        return $this->transaction->getIsolation();
    }

    public function getLastUsedAt(): int
    {
        return $this->transaction->getLastUsedAt();
    }

    private function awaitPendingNestedTransaction(): void
    {
        while ($this->busy) {
            $this->busy->getFuture()->await();
        }

        if ($this->isClosed()) {
            throw new SQLite3TransactionError('The transaction has already been committed or rolled back');
        }
    }
}
