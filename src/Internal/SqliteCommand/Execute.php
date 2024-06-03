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

namespace Amp\SQlite\Internal\SqliteCommand;

use Amp\Sql\Common\SqlCommandResult;
use Amp\SQLite3\SQLite3QueryError;
use Amp\SQLite3\Internal\SQLite3Client;
use Amp\SQLite3\Internal\SqliteCommand;
use Amp\SQLite3\Internal\SQLite3PooledResult;

final class Execute implements SqliteCommand
{
    
    public function __construct(private string $query, private array $bindings)
    {
    }

    public function execute(SQLite3Client $sqlite): mixed
    {
        $statement = $sqlite->prepare($this->query);

        if (!$statement) {
            return new SQLite3QueryError($sqlite->lastErrorMsg());
        }

        foreach ($this->bindings as $key => $value) {
            // https://www.php.net/manual/ru/function.ctype-print.php#123095
            if (\is_string($value) && \ctype_print($value)) {
                $statement->bindValue($key, $value);
            } else {
                $statement->bindValue($key, $value, SQLITE3_BLOB);
            }
        }

        $result = $statement->execute();

        if (!$result) {
            return new SQLite3QueryError($sqlite->lastErrorMsg());
        }

        if ($result->numColumns() > 0 )

        // https://www.php.net/manual/ru/sqlite3result.fetcharray.php#120631
        if ($results->numColumns() > 0) {
            $rows = [];

            while ($row = $results->fetchArray(SQLITE3_ASSOC)) {
                $rows[] = $row;
            }

            $results->finalize();

            return new ResultSetResponse($rows);
        }

        $results->finalize();

        return new CommandResultResponse($environment->getClient()->changes());

        new SqlCommandResult;
        return new SQLite3PooledResult() $statement->execute();
    }
}
