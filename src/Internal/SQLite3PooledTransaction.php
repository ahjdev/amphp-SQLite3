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

use Revolt\EventLoop;
use Amp\SQLite3\SQLite3Result;
use Amp\SQLite3\SQLite3Statement;
use Amp\SQLite3\SQLite3Transaction;
use Amp\Sql\SqlTransactionIsolation;

/**
 * @template TResult of SQLite3Result
 * @template TStatement of SQLite3Statement
 * @template TTransaction of SQLite3Transaction
 *
 * @implements SQLite3Transaction<TResult, TStatement, TTransaction>
 */
final class SQLite3PooledTransaction implements SQLite3Transaction
{
    /** @var \Closure():void */
    private readonly \Closure $release;

    private int $refCount = 1;

    /**
     * @param TTransaction $transaction Transaction object created by pooled connection.
     * @param \Closure():void $release Callable to be invoked when the transaction completes or is destroyed.
     */
    public function __construct(private readonly SQLite3Transaction $transaction, \Closure $release)
    {
        $refCount = &$this->refCount;
        $this->release = static function () use (&$refCount, $release): void {
            if (--$refCount === 0) {
                $release();
            }
        };

        $this->transaction->onClose($this->release);

        if (!$this->transaction->isActive()) {
            $this->close();
        }
    }

    /**
     * @param TTransaction $transaction
     * @param \Closure():void $release
     *
     * @return TTransaction
     */
    private function createTransaction(SQLite3Transaction $transaction, \Closure $release): SQLite3Transaction
    {
        return new SQLite3PooledTransaction($transaction, $release);
    }

    /**
     * Creates a Statement of the appropriate type using the Statement object returned by the Transaction object and
     * the given release callable.
     *
     * @param TStatement $statement
     * @param \Closure():void $release
     *
     * @return TStatement
     */
    private function createStatement(SQLite3Statement $statement, \Closure $release, ?\Closure $awaitBusyResource = null): SQLite3Statement
    {
        return new SQLite3PooledStatement($statement, $release, $awaitBusyResource);
    }

    /**
     * Creates a Result of the appropriate type using the Result object returned by the Link object and the
     * given release callable.
     *
     * @param TResult $result
     * @param \Closure():void $release
     *
     * @return TResult
     */
    private function createResult(SQLite3Result $result, \Closure $release): SQLite3Result
    {
        return new SQLite3PooledResult($result, $release);
    }

    public function query(string $sql): SQLite3Result
    {
        ++$this->refCount;

        try {
            $result = $this->transaction->query($sql);
            return $this->createResult($result, $this->release);
        } catch (\Throwable $exception) {
            EventLoop::queue($this->release);
            throw $exception;
        }
    }

    public function prepare(string $sql): SQLite3Statement
    {
        ++$this->refCount;

        try {
            $statement = $this->transaction->prepare($sql);
            return $this->createStatement($statement, $this->release);
        } catch (\Throwable $exception) {
            EventLoop::queue($this->release);
            throw $exception;
        }
    }

    public function execute(string $sql, array $params = []): SQLite3Result
    {
        ++$this->refCount;

        try {
            $result = $this->transaction->execute($sql, $params);
            return $this->createResult($result, $this->release);
        } catch (\Throwable $exception) {
            EventLoop::queue($this->release);
            throw $exception;
        }
    }

    public function beginTransaction(): SQLite3Transaction
    {
        ++$this->refCount;

        try {
            $transaction = $this->transaction->beginTransaction();
            return $this->createTransaction($transaction, $this->release);
        } catch (\Throwable $exception) {
            EventLoop::queue($this->release);
            throw $exception;
        }
    }

    public function isClosed(): bool
    {
        return $this->transaction->isClosed();
    }

    /**
     * Rolls back the transaction if it has not been committed.
     */
    public function close(): void
    {
        $this->transaction->close();
    }

    public function onClose(\Closure $onClose): void
    {
        $this->transaction->onClose($onClose);
    }

    public function isActive(): bool
    {
        return $this->transaction->isActive();
    }

    public function commit(): void
    {
        $this->transaction->commit();
    }

    public function rollback(): void
    {
        $this->transaction->rollback();
    }

    public function onCommit(\Closure $onCommit): void
    {
        $this->transaction->onCommit($onCommit);
    }

    public function onRollback(\Closure $onRollback): void
    {
        $this->transaction->onRollback($onRollback);
    }

    public function getSavepointIdentifier(): ?string
    {
        return $this->transaction->getSavepointIdentifier();
    }

    public function getIsolation(): SqlTransactionIsolation
    {
        return $this->transaction->getIsolation();
    }

    public function getLastUsedAt(): int
    {
        return $this->transaction->getLastUsedAt();
    }
}
