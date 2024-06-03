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

use SQLite3;
use SQLite3Stmt;
use Amp\Cache\LocalCache;
use Amp\SQLite3\SQLite3Config;
use Amp\SQLite3\SQLite3Statement;

final class SQLite3Client extends SQLite3
{
    private LocalCache $statementCache;

    private SQLite3 $SQLite3;

    public function __construct(SQLite3Config $request)
    {
        $this->SQLite3 = new SQLite3($request->getFilename(), $request->getFlags(), $request->getEncryptionKey());
    }

    public function getSQLite3(): SQLite3
    {
        return $this->SQLite3;
    }

    public function addStatement(SQLite3Stmt $statement): int
    {
        $id = \spl_object_id($statement);
        $this->statementCache->set("$id", $statement);
        return $id;
    }

    public function removeStatement(int $statementId): void
    {
        $this->statementCache->delete("$statementId");
    }

    public function getStatement(int $statementId): ?SQLite3Statement
    {
        return $this->statementCache->get("$statementId");
    }
}
