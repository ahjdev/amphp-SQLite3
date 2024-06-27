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

use \SQLite3;
use Amp\SQLite3\SQLite3Config;
use Amp\SQLite3\SQLite3Exception;
use Amp\SQLite3\Internal\SQLite3Worker\StatementQueue;

final class SQLite3Client
{
    private StatementQueue $statement;
    private SQLite3 $SQLite3;

    public function __construct(SQLite3Config $request)
    {
        $this->SQLite3   = new SQLite3($request->getFilename(), $request->getFlags(), $request->getEncryptionKey());
        $this->statement = new StatementQueue;
    }

    public function getSQLite3(): SQLite3
    {
        return $this->SQLite3;
    }

    public function getLastError(string $class = SQLite3Exception::class)
    {
        $class = \is_subclass_of($class, \Exception::class) ? $class : SQLite3Exception::class;
        return new $class(
            $this->SQLite3->lastErrorMsg(),
            $this->SQLite3->lastErrorCode(),
        );
    }

    public function getLastInsertId(): int
    {
        return $this->SQLite3->lastInsertRowID();
    }

    public function getAffectedRows(): int
    {
        return $this->SQLite3->changes();
    }

    public function getStatements(): StatementQueue
    {
        return $this->statement;
    }
}
