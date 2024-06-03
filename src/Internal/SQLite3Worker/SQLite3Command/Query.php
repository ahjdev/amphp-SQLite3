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

namespace Amp\SQlite\Internal\SQLite3Worker\SqliteCommand;

use Amp\SQLite3\Internal\SQLite3Client;
use Amp\SQLite3\Internal\SQLite3Worker\SQLite3Command;
use Amp\SQLite3\Internal\SQLite3Worker\SQLite3Response\SQLite3WorkerCommandResult;
use Amp\SQLite3\Internal\SQLite3Worker\SQLite3Response\SQLite3WorkerResult;
use Amp\SQLite3\SQLite3Exception;

final class Internal_SQLite3Worker_SQLite3Command_Query implements SQLite3Command
{
    public function __construct(private string $query)
    {
    }

    public function execute(SQLite3Client $client): mixed
    {
        $result = $client->getSQLite3()->query($this->query);

        if (!$result) {
            return $client->getLastError(SQLite3Exception::class);
        }

        [$affectedRows, $lastInsertId] = [$client->getAffectedRows(), $client->getLastInsertId()];

        if ($result->numColumns() === 0) {
            return new SQLite3WorkerCommandResult($affectedRows, $lastInsertId);
        }

        return new SQLite3WorkerResult($result, $affectedRows, $lastInsertId);

    }
}
