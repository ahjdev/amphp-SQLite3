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

namespace Amp\SQLite3\Internal\SQLite3Command;

use Amp\SQLite3\SQLite3QueryError;
use Amp\SQLite3\Internal\SQLite3Client;
use Amp\SQLite3\Internal\SQLite3Command;

final class StatementClose implements SQLite3Command
{
    public function __construct(
        private int $statementId
    ) {
    }

    public function execute(SQLite3Client $sqlite): mixed
    {
        $statement = $sqlite->getStatement($this->statementId);

        if (!$statement) {
            return new FailureExceptionResponse("could not find statement {$this->statementId}");
        }

        $statement->close();

        $sqlite->removeStatement($this->statementId);

        return new SuccessResponse();
    }
}
