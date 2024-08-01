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
use Amp\SQLite3\Internal\SQLite3ChannelResutlArray;
use Amp\SQLite3\Internal\SQLite3Client;
use Amp\SQLite3\Internal\SQLite3Command;

final class ResultOperation extends SQLite3Command
{
    public function __construct(private string $uniqid, private string $operation)
    {
    }

    public function execute(SQLite3Client $SQLite3Client): mixed
    {
        $result = $SQLite3Client->getResult($this->uniqid);

        if ($result === null) {
            return new SQLite3ChannelException("Could not find result {$this->uniqid}");
        }

        switch ($this->operation) {
            case 'reset':
                return $result->reset();

            case 'finalize':
                $SQLite3Client->removeResult($this->uniqid);
                return $result->finalize();

            case 'fetchArray':
                $result = $result->fetchArray(SQLITE3_ASSOC);
                return new SQLite3ChannelResutlArray($result);

            default:
                return new SQLite3ChannelException("Invalid result operation {$this->uniqid}");
        };
    }
}
