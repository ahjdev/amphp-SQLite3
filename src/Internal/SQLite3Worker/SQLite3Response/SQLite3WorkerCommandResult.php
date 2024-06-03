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

namespace Amp\SQLite3\Internal\SQLite3Worker\SQLite3Response;

use Amp\SQLite3\SQLite3Result;

/**
 * @template TFieldValue
 * @template TResult of SQLite3Result
 * @implements SQLite3Result<TFieldValue>
 */
final class SQLite3WorkerCommandResult implements SQLite3Result
{
    private ?int $lastInsertId;

    public function __construct(private int $affectedRows, int $lastInsertId)
    {
        $this->lastInsertId = $lastInsertId ?: null;
    }

    final public function getIterator(): \EmptyIterator
    {
        return new \EmptyIterator;
    }

    /**
     * @return null Always returns null for command results.
     */
    final public function fetchRow(): ?array
    {
        return null;
    }

    public function getNextResult(): null
    {
        return null;
    }

    /**
     * @return int Returns the number of rows affected by the command.
     */
    final public function getRowCount(): int
    {
        return $this->affectedRows;
    }

    /**
     * @return null Always returns null for command results.
     */
    final public function getColumnCount(): ?int
    {
        return null;
    }

    /**
     * @return int|null Insert ID of the last auto increment row or null if not applicable to the query.
     */
    public function getLastInsertId(): ?int
    {
        return $this->lastInsertId;
    }
}
