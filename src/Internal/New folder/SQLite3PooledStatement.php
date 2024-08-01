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

use Amp\SQLite3\SQLite3Exception;
use Amp\SQLite3\SQLite3Result;
use Amp\SQLite3\SQLite3Statement;
use Revolt\EventLoop;

/**
 * @template TResult of SQLite3Result
 * @template TStatement of SQLite3Statement<TResult>
 * @implements SQLite3Statement<TResult>
 */
final class SQLite3PooledStatement implements SQLite3Statement
{
    /** @var null|\Closure():void */
    private ?\Closure $release;

    private int $refCount = 1;

    /**
     * @param TStatement $statement Statement object created by pooled connection.
     * @param \Closure():void $release Callable to be invoked when the statement and any associated results are
     *     destroyed.
     * @param (\Closure():void)|null $awaitBusyResource Callable invoked before executing the statement, which should
     *     wait if the parent resource is busy with another action (e.g., a nested transaction).
     */
    public function __construct(
        private readonly SQLite3Statement $statement,
        \Closure $release,
        private readonly ?\Closure $awaitBusyResource = null,
    ) {
        $refCount = &$this->refCount;
        $this->release = static function () use (&$refCount, $release): void {
            if (--$refCount === 0) {
                $release();
            }
        };
    }

    public function __destruct()
    {
        $this->dispose();
    }

    /**
     * @return TResult
     */
    public function execute(array $params = []): SQLite3Result
    {
        if (!$this->release) {
            throw new SQLite3Exception('The statement has been closed');
        }

        $this->awaitBusyResource && ($this->awaitBusyResource)();

        $result = $this->statement->execute($params);

        ++$this->refCount;
        return $this->createResult($result, $this->release);
    }

    private function dispose(): void
    {
        if ($this->release) {
            EventLoop::queue($this->release);
            $this->release = null;
        }
    }

    public function isClosed(): bool
    {
        return $this->statement->isClosed();
    }

    public function close(): void
    {
        $this->dispose();
        $this->statement->close();
    }

    public function onClose(\Closure $onClose): void
    {
        $this->statement->onClose($onClose);
    }

    public function getQuery(): string
    {
        return $this->statement->getQuery();
    }

    public function getLastUsedAt(): int
    {
        return $this->statement->getLastUsedAt();
    }

    /**
     * Creates a Result of the appropriate type using the Result object returned by the Statement object and the
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
}
