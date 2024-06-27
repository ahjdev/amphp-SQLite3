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

use SQLite3Stmt;
use Amp\SQLite3\SQLite3Result;
use Amp\SQLite3\Internal\SQLite3Client;
use Amp\SQLite3\Internal\SQLite3ResultProxy;
use Amp\SQLite3\Internal\SQLite3CommandResult;

abstract class SQLite3Command
{
    abstract public function execute(SQLite3Client $client): mixed;

    public function bindValues(SQLite3Stmt $statement, array $bindings): void
    {
        foreach ($bindings as $key => $value) {
            if (\is_string($value) && !\ctype_print($value)) {
                $a = $statement->bindValue($key, $value, SQLITE3_BLOB);
            } else {
                $a = $statement->bindValue($key, $value);
            }
        }
    }

    public function bindExecute(SQLite3Client $client, SQLite3Stmt $statement, array $bindings): SQLite3Result|false
    {
        $this->bindValues($statement, $bindings);
        return $this->createResult($client, $statement->execute());
    }

    public function createResult(SQLite3Client $client, \SQLite3Result $result): SQLite3ResultProxy|SQLite3CommandResult
    {
        [$columnCount, $affectedRows, $lastInsertId] = [
            $result->numColumns(),
            $client->getAffectedRows(),
            $client->getLastInsertId()
        ];
        if ($columnCount === 0) {
            return new SQLite3CommandResult($affectedRows, $lastInsertId);
        }
        return new SQLite3ResultProxy($result, $columnCount, $affectedRows, $lastInsertId);
    }
}
