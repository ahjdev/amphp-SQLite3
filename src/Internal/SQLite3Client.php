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

use Amp\Cache\Cache;
use Amp\Cache\LocalCache;
use Amp\SQLite3\SQLite3Config;

final class SQLite3Client
{
    /** @var Cache<\SQLite3Result> */
    private Cache $result;

    /** @var Cache<\SQLite3Stmt> */
    private Cache $stmt;

    private \SQLite3 $SQLite3;

    public function __construct(SQLite3Config $request)
    {
        $this->stmt    = new LocalCache();
        $this->result  = new LocalCache();
        $this->SQLite3 = new \SQLite3($request->getFilename(), $request->getFlags(), $request->getEncryptionKey());
        $this->SQLite3->enableExceptions(true);
    }

    public function query(string $query): \SQLite3Result
    {
        return $this->SQLite3->query($query);
    }

    public function prepare(string $query): \SQLite3Stmt
    {
        return $this->SQLite3->prepare($query);
    }

    public function close(): bool
    {
        return $this->SQLite3->close();
    }

    public function getLastInsertId(): ?int
    {
        return $this->SQLite3->lastInsertRowID() ?: null;
    }

    public function getAffectedRows(): int
    {
        return $this->SQLite3->changes();
    }

    public function addStatment(\SQLite3Stmt $stmt): string
    {
        $uniqid = uniqid();
        $this->stmt->set($uniqid, $stmt);
        return $uniqid;
    }

    public function removeStatement(string $uniqid)
    {
        $this->stmt->delete($uniqid);
    }

    public function getStatement(string $uniqid): \SQLite3Stmt
    {
        return $this->stmt->get($uniqid);
    }
    public function addResult(\SQLite3Result $result): string
    {
        $uniqid = uniqid();
        $this->result->set($uniqid, $result);
        return $uniqid;
    }

    public function removeResult(string $uniqid)
    {
        $this->result->delete($uniqid);
    }

    public function getResult(string $uniqid): \SQLite3Result
    {
        return $this->result->get($uniqid);
    }
}
