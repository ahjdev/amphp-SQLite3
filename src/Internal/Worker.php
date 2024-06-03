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

namespace Amp\SQlite\Internal;

use Amp\Sync\Channel;
use Amp\SQlite\SQLite3Config;

return function (Channel $channel): mixed
{
    $config = $channel->receive();
    \assert($config instanceof SQLite3Config);
    $environment = new SqliteClient($config);

    try {
        while (true) {
            $command = $channel->receive();
            \assert($command instanceof Command);
            $response = $command->execute($environment);
            $channel->send($response);
        }
    } catch (CloseConnectionException) {
        return null;
    }
};
