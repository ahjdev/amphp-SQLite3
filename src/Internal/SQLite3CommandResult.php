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
use Amp\Sql\Common\SqlCommandResult;
use Amp\SQLite3\Internal\SQLite3Worker\SQLite3WorkerCommandResult;
use Amp\SQLite3\SQLite3Result;

/**
 * @template TFieldValue
 * @template TResult of SQLite3Result
 * @implements SQLite3Result<TFieldValue>
 * @implements \IteratorAggregate<int, never>
 */
final class SQLite3CommandResult extends SqlCommandResult implements SQLite3Result, \IteratorAggregate
{
    private ?int $lastInsertId;

    public function __construct(SQLite3WorkerCommandResult $result)
    {
        /** @var Future<SQLite3Result|null> $future Explicit declaration for Psalm. */
        $future = Future::complete();
        parent::__construct($result->getRowCount(), $future);
        $this->lastInsertId = $result->getLastInsertId() ?: null; // Convert 0 to null
    }

    /**
     * Changes return type to this library's Result type.
     */
    public function getNextResult(): ?SQLite3Result
    {
        return parent::getNextResult();
    }

    /**
     * @return int|null Insert ID of the last auto increment row or null if not applicable to the query.
     */
    public function getLastInsertId(): ?int
    {
        return $this->lastInsertId;
    }
}
