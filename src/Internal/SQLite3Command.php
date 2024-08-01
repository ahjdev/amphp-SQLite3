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

abstract class SQLite3Command
{
    abstract public function execute(SQLite3Client $client): mixed;

    protected function bindValues(\SQLite3Stmt $stmt, array $bindings): bool
    {
        foreach ($bindings as $key => $value) {
            if (\is_string($value) && !\ctype_print($value)) {
                $bind = $stmt->bindValue($key, $value, SQLITE3_BLOB);
            } else {
                $bind = $stmt->bindValue($key, $value);
            }
        }
        return true;
    }

    protected function bindExecute(SQLite3Client $SQLite3Client, \SQLite3Stmt $stmt, array $bindings): SQLite3ChannelException|SQLite3ChannelResult
    {
        try {
            $this->bindValues($stmt, $bindings);
            $result = $stmt->execute();
            return $this->createResult($SQLite3Client, $result);
        } catch (\Throwable $error) {
            $query = $stmt->getSQL(true);
            return $this->createError($error, $query);
        }
    }

    public function createResult(SQLite3Client $SQLite3Client, \SQLite3Result $result)
    {
        $uniqid = $SQLite3Client->addResult($result);

        [ $columnCount, $lastInsertId, $affectedRows ] = [
            $result->numColumns(),
            $SQLite3Client->getLastInsertId(),
            $SQLite3Client->getAffectedRows()
        ];

        return new SQLite3ChannelResult($uniqid, $columnCount, $lastInsertId, $affectedRows);
    }

    protected function createError(\Throwable $error, string $query = ''): SQLite3ChannelException
    {
        $message = $error->getMessage();
        $code    = $error->getCode();
        return new SQLite3ChannelException($message, $code, $query);
    }
}
