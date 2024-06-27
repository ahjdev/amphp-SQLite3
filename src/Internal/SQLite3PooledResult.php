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

use Amp\Future;
use Amp\Sql\SqlResult;
use Amp\SQLite3\SQLite3Result;
use Revolt\EventLoop;
use function Amp\async;

/**
 * @template TFieldValue
 * @template TResult of SQLite3Result
 * @implements SQLite3Result<TFieldValue>
 * @implements \IteratorAggregate<int, array<string, TFieldValue>>
 */
final class SQLite3PooledResult implements SQLite3Result, \IteratorAggregate
{
    /** @var Future<TResult|null>|null */
    private ?Future $next = null;

    /** @var \Iterator<int, array<string, TFieldValue>> */
    private readonly \Iterator $iterator;

    /**
     * @param TResult $result Result object created by pooled connection or statement.
     * @param \Closure():void $release Callable to be invoked when the result set is destroyed.
     */
    public function __construct(private readonly SQLite3Result $result, private readonly \Closure $release)
    {
        if ($this->result instanceof SQLite3CommandResult) {
            $this->iterator = $this->result->getIterator();
            $this->next = self::fetchNextResult($this->result, $this->release);
            return;
        }

        $next = &$this->next;
        $this->iterator = (static function () use (&$next, $result, $release): \Generator {
            try {
                // Using foreach loop instead of yield from to avoid PHP bug,
                // see https://github.com/amphp/mysql/issues/133
                foreach ($result as $row) {
                    yield $row;
                }
            } catch (\Throwable $exception) {
                if (!$next) {
                    EventLoop::queue($release);
                }
                throw $exception;
            }

            $next ??= self::fetchNextResult($result, $release);
        })();
    }

    public function __destruct()
    {
        EventLoop::queue(self::dispose(...), $this->iterator);
    }

    private static function dispose(\Iterator $iterator): void
    {
        try {
            // Discard remaining rows in the result set.
            while ($iterator->valid()) {
                $iterator->next();
            }
        } catch (\Throwable) {
            // Ignore errors while discarding result.
        }
    }

    public function getIterator(): \Traversable
    {
        return $this->iterator;
    }

    public function fetchRow(): ?array
    {
        if (!$this->iterator->valid()) {
            return null;
        }

        $current = $this->iterator->current();
        $this->iterator->next();
        return $current;
    }

    public function getRowCount(): ?int
    {
        return $this->result->getRowCount();
    }

    public function getColumnCount(): ?int
    {
        return $this->result->getColumnCount();
    }

    /**
     * @return TResult|null
     */
    public function getNextResult(): ?SQLite3Result
    {
        $this->next ??= self::fetchNextResult($this->result, $this->release);
        return $this->next->await();
    }

    public function getLastInsertId(): ?int
    {
        return $this->result->getLastInsertId();
    }

    /**
     * @template Tr of SQLite3Result
     *
     * @param Tr $result
     * @param \Closure():void $release
     *
     * @return Future<Tr|null>
     */
    private static function fetchNextResult(SQLite3Result $result, \Closure $release): Future
    {
        return async(static function () use ($result, $release): ?SQLite3Result {
            /** @var Tr|null $result */
            $result = $result->getNextResult();

            if ($result === null) {
                EventLoop::queue($release);
                return null;
            }

            return static::newInstanceFrom($result, $release);
        });
    }

    /**
     * @template Tr of SqlResult
     *
     * @param Tr $result
     * @param \Closure():void $release
     *
     * @return Tr
     */
    protected static function newInstanceFrom(SQLite3Result $result, \Closure $release): self
    {
        return new self($result, $release);
    }
}
