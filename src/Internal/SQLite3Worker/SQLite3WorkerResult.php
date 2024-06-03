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

namespace Amp\SQLite3\Internal\SQLite3Worker;

use Amp\SQLite3\SQLite3Result;

final class SQLite3WorkerResult implements SQLite3Result, \IteratorAggregate
{
    private int $columnCount;

    private array $result = [];

    public function __construct(\SQLite3Result $result, private int $affectedRows, private int $lastInsertId)
    {
        $this->columnCount = $result->numColumns();
        while ($array = $result->fetchArray(SQLITE3_ASSOC)) {
            $this->result[] = $array;
        }
    }

    public function getIterator(): \Generator
    {
        return yield from $this->result;
    }

    public function fetchRow(): ?array
    {
        return null;
    }

    public function getNextResult(): null
    {
        return null;
    }

    public function getRowCount(): int
    {
        return $this->affectedRows;
    }

    public function getColumnCount(): ?int
    {
        return $this->columnCount;
    }

    public function getLastInsertId(): ?int
    {
        return $this->lastInsertId;
    }
}
