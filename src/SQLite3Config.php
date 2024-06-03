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

namespace Amp\SQLite3;

final class SQLite3Config
{
    public function __construct(private string $filename, private int $flags = SQLITE3_OPEN_READWRITE | SQLITE3_OPEN_CREATE, private string $encryptionKey = '')
    {
        if (!\extension_loaded('sqlite3')) {
            throw new SQLite3Exception(__CLASS__ . " requires the sqlite3 extension");
        }
    }

    final public function getFileName(): string
    {
        return $this->filename;
    }

    final public function withFileName(string $filename): static
    {
        $new = clone $this;
        $new->filename = $filename;
        return $new;
    }

    final public function getFlags(): int
    {
        return $this->flags;
    }

    final public function withFlags(int $flags): static
    {
        $new = clone $this;
        $new->flags = $flags;
        return $new;
    }

    final public function getEncryptionKey(): string
    {
        return $this->encryptionKey;
    }

    final public function withEncryptionKey(string $encryptionKey): static
    {
        $new = clone $this;
        $new->encryptionKey = $encryptionKey;
        return $new;
    }
}
