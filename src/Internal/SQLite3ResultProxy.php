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

use Amp\Sql\SqlResult;
use Amp\SQLite3\SQLite3Result;

final class SQLite3ResultProxy implements SqlResult, \IteratorAggregate
{
    private SQLite3Result $result;

    public function __construct(
        array $result,
        SQLite3ConnectionResult $result
    ) {
        $this->result = $result;
    }

    public function getIterator(): \Traversable
    {
        // Using a Generator to keep a reference to $this.
        yield from $this->generator;
    }

    public function fetchRow(): ?array
    {
        if (!$this->generator->valid()) {
            return null;
        }

        $current = $this->generator->current();
        $this->generator->next();
        return $current;
    }

    public function getNextResult(): ?SQLite3Result
    {
        $this->nextResult ??= async(function (): ?SQLite3Result {
            self::dispose($this->generator);

            $deferred = $this->result->next ??= new DeferredFuture;
            $result = $deferred->getFuture()->await();

            if ($result instanceof MysqlResultProxy) {
                return new self($result);
            }

            return $result; // Instance of CommandResult or null.
        });

        return $this->nextResult->await();
    }

    public function getRowCount(): ?int
    {
        return $this->result->getRowCount();
    }

    public function getColumnCount(): int
    {
        return $this->result->getColumnCount();
    }

    public function getLastInsertId(): ?int
    {
        return $this->result->getLastInsertId();
    }
}
