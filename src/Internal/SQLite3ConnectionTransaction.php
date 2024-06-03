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
use Amp\Sql\Common\SqlNestableTransactionExecutor;
use Amp\Sql\SqlTransactionIsolation;
use Amp\SQLite3\SQLite3Exception;
use Amp\SQLite3\SQLite3Result;
use Amp\SQLite3\SQLite3Statement;
use Amp\SQLite3\SQLite3Transaction;
use Amp\SQLite3\SQLite3TransactionError;
use Revolt\EventLoop;

/**
 * @template TResult of SQLite3Result
 * @template TStatement of SQLite3Statement<TResult>
 * @template TTransaction of SQLite3Transaction
 * @template TNestedExecutor of SqlNestableTransactionExecutor<TResult, TStatement>
 *
 * @implements SQLite3Transaction<TResult, TStatement, TTransaction>
 */
final class SQLite3ConnectionTransaction implements SQLite3Transaction
{
    /** @var \Closure():void */
    private readonly \Closure $release;

    private int $refCount = 1;

    private bool $active = true;

    private readonly DeferredFuture $onCommit;
    private readonly DeferredFuture $onRollback;
    private readonly DeferredFuture $onClose;

    private ?DeferredFuture $busy = null;

    protected function createStatement(SQLite3Statement $statement, \Closure $release, ?\Closure $awaitBusyResource = null): SQLite3Statement
    {
        return new SQLite3PooledStatement($statement, $release, $awaitBusyResource);
    }

    protected function createResult(SQLite3Result $result, \Closure $release): SQLite3Result
    {
        return new SQLite3PooledResult($result, $release);
    }

    protected function createNestedTransaction(
        SQLite3Transaction $transaction,
        SQLite3NestableExecutor $executor,
        string $identifier,
        \Closure $release,
    ): SQLite3Transaction {
        return new SQLite3NestedTransaction($transaction, $executor, $identifier, $release);
    }

    /**
     * @param TNestedExecutor $executor
     * @param \Closure():void $release
     */
    public function __construct(
        private readonly SqlNestableTransactionExecutor $executor,
        \Closure $release,
        private readonly SqlTransactionIsolation $isolation,
    ) {
        $busy = &$this->busy;
        $refCount = &$this->refCount;
        $this->release = static function () use (&$busy, &$refCount, $release): void {
            $busy?->complete();
            $busy = null;

            if (--$refCount === 0) {
                $release();
            }
        };

        $this->onCommit   = new DeferredFuture();
        $this->onRollback = new DeferredFuture();
        $this->onClose    = new DeferredFuture();
        $this->onClose($this->release);
    }

    public function __destruct()
    {
        if (!$this->active) {
            return;
        }

        if ($this->executor->isClosed()) {
            $this->onRollback->complete();
            $this->onClose->complete();
        }

        $busy       = &$this->busy;
        $executor   = $this->executor;
        $onClose    = $this->onClose;
        $onRollback = $this->onRollback;
        EventLoop::queue(static function () use (&$busy, $executor, $onRollback, $onClose): void {
            try {
                while ($busy) {
                    $busy->getFuture()->await();
                }

                if (!$executor->isClosed()) {
                    $executor->rollback();
                }
            } catch (SQLite3Exception) {
                // Ignore failure if connection closes during query.
            } finally {
                $onRollback->complete();
                $onClose->complete();
            }
        });
    }

    public function getLastUsedAt(): int
    {
        return $this->executor->getLastUsedAt();
    }

    public function getSavepointIdentifier(): ?string
    {
        return null;
    }

    /**
     * Closes and rolls back all changes in the transaction.
     */
    public function close(): void
    {
        if (!$this->active) {
            return;
        }
        $this->rollback(); // Invokes $this->release callback.
    }

    public function isClosed(): bool
    {
        return $this->onClose->isComplete();
    }

    public function onClose(\Closure $onClose): void
    {
        $this->onClose->getFuture()->finally($onClose);
    }

    /**
     * @return bool True if the transaction is active, false if it has been committed or rolled back.
     */
    public function isActive(): bool
    {
        return $this->active && !$this->executor->isClosed();
    }

    public function getIsolation(): SqlTransactionIsolation
    {
        return $this->isolation;
    }

    /**
     * @throws SQLite3TransactionError If the transaction has been committed or rolled back.
     */
    public function query(string $sql): SQLite3Result
    {
        $this->awaitPendingNestedTransaction();

        ++$this->refCount;
        try {
            $result = $this->executor->query($sql);
        } catch (\Throwable $exception) {
            EventLoop::queue($this->release);
            throw $exception;
        }

        return $this->createResult($result, $this->release);
    }

    /**
     * @throws SQLite3TransactionError If the transaction has been committed or rolled back.
     *
     * @psalm-suppress InvalidReturnStatement, InvalidReturnType
     */
    public function prepare(string $sql): SQLite3Statement
    {
        $this->awaitPendingNestedTransaction();

        ++$this->refCount;
        try {
            $statement = $this->executor->prepare($sql);
        } catch (\Throwable $exception) {
            EventLoop::queue($this->release);
            throw $exception;
        }

        $busy = &$this->busy;
        return $this->createStatement($statement, $this->release, static function () use (&$busy): void {
            while ($busy) {
                $busy->getFuture()->await();
            }
        });
    }

    /**
     * @throws SQLite3TransactionError If the transaction has been committed or rolled back.
     */
    public function execute(string $sql, array $params = []): SQLite3Result
    {
        $this->awaitPendingNestedTransaction();

        ++$this->refCount;
        try {
            $result = $this->executor->execute($sql, $params);
        } catch (\Throwable $exception) {
            EventLoop::queue($this->release);
            throw $exception;
        }

        return $this->createResult($result, $this->release);
    }

    public function beginTransaction(): SQLite3Transaction
    {
        $this->awaitPendingNestedTransaction();

        ++$this->refCount;
        $this->busy = new DeferredFuture();
        try {
            $identifier = \bin2hex(\random_bytes(8));
            $this->executor->createSavepoint($identifier);
        } catch (\Throwable $exception) {
            EventLoop::queue($this->release);
            throw $exception;
        }

        /** @psalm-suppress InvalidArgument Recursive templates prevent satisfying this call. */
        return $this->createNestedTransaction($this, $this->executor, $identifier, $this->release);
    }

    /**
     * Commits the transaction and makes it inactive.
     *
     * @throws SQLite3TransactionError If the transaction has been committed or rolled back.
     */
    public function commit(): void
    {
        $this->active = false;
        $this->awaitPendingNestedTransaction();

        try {
            $this->executor->commit();
        } finally {
            $this->onCommit->complete();
            $this->onClose->complete();
        }
    }

    /**
     * Rolls back the transaction and makes it inactive.
     *
     * @throws SQLite3TransactionError If the transaction has been committed or rolled back.
     */
    public function rollback(): void
    {
        $this->active = false;
        $this->awaitPendingNestedTransaction();

        try {
            $this->executor->rollback();
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

    private function awaitPendingNestedTransaction(): void
    {
        while ($this->busy) {
            $this->busy->getFuture()->await();
        }

        if ($this->isClosed()) {
            throw new SQLite3TransactionError("The transaction has been committed or rolled back");
        }
    }
}
