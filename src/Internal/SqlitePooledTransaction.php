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

namespace Amp\SQlite\Internal;

use Revolt\EventLoop;
use Amp\SQlite\SqliteResult;
use Amp\SQlite\SqliteStatement;
use Amp\SQlite\SqliteTransaction;
use Amp\Sql\SqlTransactionIsolation;

/**
 * @template TResult of SqliteResult
 * @template TStatement of SqliteStatement
 * @template TTransaction of SqliteTransaction
 *
 * @implements SqliteTransaction<TResult, TStatement, TTransaction>
 */
final class SqlitePooledTransaction implements SqliteTransaction
{
    /** @var \Closure():void */
    private readonly \Closure $release;

    private int $refCount = 1;

    /**
     * @param TTransaction $transaction Transaction object created by pooled connection.
     * @param \Closure():void $release Callable to be invoked when the transaction completes or is destroyed.
     */
    public function __construct(private readonly SqliteTransaction $transaction, \Closure $release)
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
    private function createTransaction(SqliteTransaction $transaction, \Closure $release): SqliteTransaction
    {
        return new SqlitePooledTransaction($transaction, $release);
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
    private function createStatement(SqliteStatement $statement, \Closure $release, ?\Closure $awaitBusyResource = null): SqliteStatement
    {
        return new SqlitePooledStatement($statement, $release, $awaitBusyResource);
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
    private function createResult(SqliteResult $result, \Closure $release): SqliteResult
    {
        return new SqlitePooledResult($result, $release);
    }

    public function query(string $sql): SqliteResult
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

    public function prepare(string $sql): SqliteStatement
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

    public function execute(string $sql, array $params = []): SqliteResult
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

    public function beginTransaction(): SqliteTransaction
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
