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

use Amp\SQLite3\Internal\SQLite3Worker\SQLite3Command;
use Amp\SQLite3\SQLite3Config;
use Amp\SQLite3\SQLite3Exception;
use Amp\Sync\Channel;

return function (Channel $channel): void {
    $config = $channel->receive();
    \assert($config instanceof SQLite3Config);
    try {
        $SQLite3 = new SQLite3Client($config);
    } catch (\Throwable $e) {
        $channel->send(new SQLite3Exception("Cannot connect to SQLite3", previous: $e));
    }
    while (true) {
        $command = $channel->receive();
        try {
            \assert($command instanceof SQLite3Command);
            $result = $command->execute($SQLite3);
        } catch (\Throwable $result) {
        }
        $channel->send($result);
    }
};
