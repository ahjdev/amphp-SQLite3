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
use Amp\SQLite3\SQLite3Config;
use Amp\SQLite3\SQLite3ConnectionPool;
use Amp\SQLite3\SQLite3Exception;
use Amp\SQLite3\SQLite3Result;
use Amp\SQLite3\SQLite3Statement;
use Amp\SQLite3\SQLite3Transaction;

/**
 * @template TConfig of SQLite3Config
 * @template TResult of SQLite3Result
 * @template TStatement of SQLite3Statement<TResult>
 * @template TTransaction of SQLite3Transaction
 * @implements SQLite3Statement<TResult>
 */
final class SQLite3StatementPool implements SQLite3Statement
{
    private readonly SQLite3ConnectionPool $pool;

    /** @var \SplQueue<TStatement> */
    private readonly \SplQueue $statements;
    private readonly string $sql;
    private int $lastUsedAt;

    /** @var \Closure(string):TStatement */
    private readonly \Closure $prepare;
    private readonly DeferredFuture $onClose;

    /**
     * @param SQLite3ConnectionPool<TConfig, TResult, TStatement, TTransaction> $pool Pool used to prepare statements for execution.
     * @param string                                                            $sql SQL statement to prepare
     * @param \Closure(string):TStatement                                       $prepare Callable that returns a new prepared statement.
     */
    public function __construct(SQLite3ConnectionPool $pool, string $sql, \Closure $prepare)
    {
        $this->lastUsedAt = \time();
        $this->statements = $statements = new \SplQueue;
        $this->pool       = $pool;
        $this->prepare    = $prepare;
        $this->sql        = $sql;
        $this->onClose    = $onClose = new DeferredFuture();

        // $timeoutWatcher = EventLoop::repeat(1, static function () use ($pool, $statements): void {
        //     $now = \time();
        //     $idleTimeout = ((int) ($pool->getIdleTimeout() / 10)) ?: 1;

        //     while (!$statements->isEmpty()) {
        //         $statement = $statements->bottom();
        //         \assert($statement instanceof SQLite3Statement);

        //         if ($statement->getLastUsedAt() + $idleTimeout > $now) {
        //             return;
        //         }

        //         $statements->shift();
        //     }
        // });

        // EventLoop::unreference($timeoutWatcher);
        // $this->onClose(static fn () => EventLoop::cancel($timeoutWatcher));

        $this->pool->onClose(static fn () => $onClose->isComplete() || $onClose->complete());
    }

    public function __destruct()
    {
        $this->close();
    }

    /**
     * Unlike regular statements, as long as the pool is open this statement will not die.
     *
     * @return TResult
     */
    public function execute(array $params = []): SQLite3Result
    {
        if ($this->isClosed()) {
            throw new SQLite3Exception('The statement has been closed or the connection pool has been closed');
        }

        $this->lastUsedAt = \time();

        $statement = $this->pop();

        try {
            $result = $statement->execute($params);
        } catch (\Throwable $exception) {
            $this->push($statement);
            throw $exception;
        }

        return $this->createResult($result, fn () => $this->push($statement));
    }

    public function close(): void
    {
        if (!$this->onClose->isComplete()) {
            $this->onClose->complete();
        }
    }

    public function isClosed(): bool
    {
        return $this->onClose->isComplete();
    }

    public function onClose(\Closure $onClose): void
    {
        $this->onClose->getFuture()->finally($onClose);
    }

    public function getQuery(): string
    {
        return $this->sql;
    }

    public function getLastUsedAt(): int
    {
        return $this->lastUsedAt;
    }

    private function createResult(SQLite3Result $result, \Closure $release): SQLite3Result
    {
        return new SQLite3PooledResult($result, $release);
    }

    /**
     * Only retains statements if less than 10% of the pool is consumed by this statement and the pool has
     * available connections.
     *
     * @param TStatement $statement
     */
    private function push(SQLite3Statement $statement): void
    {
        $this->statements->enqueue($statement);
    }

    /**
     * @return TStatement
     */
    private function pop(): SQLite3Statement
    {
        while (!$this->statements->isEmpty()) {
            $statement = $this->statements->dequeue();
            \assert($statement instanceof SQLite3Statement);

            if (!$statement->isClosed()) {
                return $statement;
            }
        }
        return ($this->prepare)($this->sql);
    }
}
