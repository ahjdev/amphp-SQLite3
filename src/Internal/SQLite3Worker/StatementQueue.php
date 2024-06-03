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

final class StatementQueue
{
    /** @var list<SQLite3Stmt> */
    public array $cache = [];

    public function __construct()
    {
    }

    public function get(int $key): ?SQLite3Stmt
    {
        if (!isset($this->cache[$key])) {
            return null;
        }
        return $this->cache[$key];
    }

    public function set(SQLite3Stmt $value): int
    {
        $key = \spl_object_id($value);
        unset($this->cache[$key]);
        $this->cache[$key] = $value;
        return $key;
    }

    public function delete(int $key): bool
    {
        $exists = isset($this->cache[$key]);
        $exists && $this->cache[$key]->close();
        unset($this->cache[$key]);
        return $exists;
    }
}
