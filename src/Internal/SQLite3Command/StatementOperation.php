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

use Amp\SQLite3\Internal\SQLite3ChannelException;
use Amp\SQLite3\Internal\SQLite3Client;
use Amp\SQLite3\Internal\SQLite3Command;

final class StatementOperation extends SQLite3Command
{
    public function __construct(private string $uniqid, private string $operation, private array $args = [])
    {
    }

    public function execute(SQLite3Client $SQLite3Client): mixed
    {
        $stmt = $SQLite3Client->getStatement($this->uniqid);

        if ($stmt === null) {
            return new SQLite3ChannelException("Could not find statement {$this->uniqid}");
        }

        switch ($this->operation) {
            case 'reset':
                return $stmt->reset();

            case 'close':
                $SQLite3Client->removeStatement($this->uniqid);
                return $stmt->close();

            case 'getSql':
                return $stmt->getSQL();

            case 'bindValue':
                return $this->bindValues($stmt, $this->args);

            case 'execute':
                return $this->bindExecute($SQLite3Client, $stmt, $this->args);

            default:
                return new SQLite3ChannelException("Invalid statement operation {$this->uniqid}");
        }
    }
}
