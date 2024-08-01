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

use Throwable;
use Amp\Sync\Channel;
use Amp\SQLite3\SQLite3Config;
use Amp\SQLite3\SQLite3ConnectionException;
use Amp\SQLite3\Internal\SQLite3Command;

return (new class {

    private SQLite3Client $client;

    public function connectToSQLite3(Channel $channel)
    {
        $context = $channel->receive();
        \assert($context instanceof SQLite3Config, 'SQLite3Config not found');
        // Connect to Sqlite3
        try {
            $this->client = new SQLite3Client($context);
        } catch (Throwable $e) {
            $channel->send(new SQLite3ConnectionException("Cannot connect to SQLite3", previous: $e));
            $channel->send(null);
        }
    }

    public function getCommand(Channel $channel): SQLite3Command|Throwable
    {
        $command = $channel->receive();
        \assert($command instanceof SQLite3Command, 'Inavlid SQLite3Command');
        return $command;
    }

    public function __invoke(Channel $channel)
    {
        $this->connectToSQLite3($channel);
        // do stuff
        while (!$channel->isClosed())
        {
            try
            {
                $command = $this->getCommand($channel);
                $respone = $command->execute($this->client);
                if ($respone === null) {
                    $channel->close();
                    return;
                }
                $channel->send($respone);
            } catch (Throwable $error)
            {
                $exception = new SQLite3ChannelStatement($error->getMessage(), $error->getCode());
                $channel->send($exception);
            }
        }
    }
})(...);
