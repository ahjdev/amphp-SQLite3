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

namespace Amp\SQLite3;

use Amp\DeferredFuture;
use Amp\Sql\SqlException;
use Amp\Sql\SqlTransaction;

use Amp\Parallel\Context\Context;
use Amp\Sql\SqlTransactionIsolation;
use Amp\Parallel\Context\StatusError;
use Amp\SQlite\Internal\SqliteCommand\Query;
use Amp\SQlite\Internal\SqliteCommand\Execute;
use Amp\SQlite\Internal\SqliteCommand\Prepare;
use function Amp\Parallel\Context\contextFactory;

/**
 * @template TConfig of SQLite3Config
 * @template TResult of SQLite3Result
 * @template TStatement of SQLite3Statement<TResult>
 * @template TTransaction of SQLite3Transaction
 * @template TConnection of SQLite3Connection<TConfig, TResult, TStatement, TTransaction>
 *
 * @implements SQLite3Connection<TConfig, TResult, TStatement, TTransaction>
 */
final class SQLite3ConnectionPool implements SQLite3Connection
{
    private Context $context;
    private readonly DeferredFuture $onClose;

    public function __construct(private readonly SQLite3Config $config)
    {
        $this->context = contextFactory()->start(__DIR__ . '/Internal/Worker.php');
        $this->context->send($config);
    }

    public function __destruct()
    {
        $this->close();
    }

    public function getTransactionIsolation(): SqlTransactionIsolation
    {
        return $this->transactionIsolation;
    }

    public function setTransactionIsolation(SqlTransactionIsolation $isolation): void
    {
        $this->transactionIsolation = $isolation;
    }

    public function getConfig(): SQLite3Config
    {
        return $this->config;
    }

    public function getLastUsedAt(): int
    {
        return 0;
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
     * Close all connections in the pool. No further queries may be made after a pool is closed.
     */
    public function close(): void
    {
        if ($this->onClose->isComplete()) {
            return;
        }
        try {
            if (!$this->context->isClosed()) {
                return;
            }
            $this->context->close();
        } catch (StatusError $e) {
            throw new SQLite3ConnectionException($e->getMessage(), $e->getCode(), $e);
        }
    
        $this->onClose->complete();
    }

    protected function createResult(SQLite3Result $result, \Closure $release): SQLite3Result
    {
        return new Internal\SQLite3PooledResult($result, $release);
    }

    protected function createStatement(SQLite3Statement $statement, \Closure $release): SQLite3Statement
    {
        return new Internal\SQLite3PooledStatement($statement, $release);
    }

    protected function createStatementPool(string $sql, \Closure $prepare): SQLite3Statement
    {
        return new Internal\SQLite3StatementPool($this, $sql, $prepare);
    }

    protected function createTransaction(SQLite3Transaction $transaction, \Closure $release): SQLite3Transaction
    {
        return new Internal\SQLite3PooledTransaction($transaction, $release);
    }

    /**
     * Prepares a new statement on an available connection.
     *
     * @return TStatement
     *
     * @throws SqlException
     */
    private function prepareStatement(string $sql): SQLite3Statement
    {
        try {
            $command = new Prepare($sql);
            $this->context->send($command);

        } catch (StatusError $e) {
            throw new SQLite3ConnectionException($e->getMessage(), $e->getCode(), $e);
        }

        return $this->createStatement($statement, fn () => $this->push($connection));
    }

    public function query(string $sql): SQLite3Result
    {
        try {
            $command = new Query($sql);
            $this->context->send($command);

            return $this->createQueryResultFromResponse($response);
        } catch (StatusError $e) {
            throw new SQLite3ConnectionException($e->getMessage(), $e->getCode(), $e);
        }

        return $this->createResult($result, fn () => $this->push($connection));
    }

    public function execute(string $sql, array $params = []): SQLite3Result
    {
        try {
        $command = new Execute($sql, $params);
        $response = $this->context->send($command);
        } catch (StatusError $e) {
            throw new SQLite3ConnectionException($e->getMessage(), $e->getCode(), $e);
        }
        return $this->createResult($result, fn () => $this->push($connection));
    }

    /**
     * Prepared statements returned by this method will stay alive as long as the pool remains open.
     */
    public function prepare(string $sql): SQLite3Statement
    {
        /** @psalm-suppress InvalidArgument Psalm is not properly detecting the templated return type. */
        return $this->createStatementPool($sql, $this->prepareStatement(...));
    }

    /**
     * Changes return type to this library's Transaction type.
     */

    public function beginTransaction(SqliteTransactionIsolationLevel $isolation = SqliteTransactionIsolationLevel::Deferred): SQLite3Transaction
    {
        $this->execute('BEGIN ' . $isolation->toSql());
        return $this->createTransaction($transaction, fn () => $this->push($connection));
    }
}
