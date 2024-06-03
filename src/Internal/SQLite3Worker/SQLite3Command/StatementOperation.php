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

namespace Amp\SQLite3\Internal\SQLite3Worker\SQLite3Command;

use Amp\SQLite3\Internal\SQLite3Client;
use Amp\SQLite3\Internal\SQLite3Worker\SQLite3Command;
use Amp\SQLite3\SQLite3Exception;

final class StatementOperation extends SQLite3Command
{
    public function __construct(private int $id, private string $operation, private array|bool $args = [])
    {
    }

    public function execute(SQLite3Client $client): mixed
    {
        if ($state = $client->getStatements()->get($this->id)) {
            return match ($this->operation) {
                'reset'     => $state->reset() ?: $client->getLastError(),
                'getSql'    => $state->getSQL() ?: $client->getLastError(),
                'close'     => $client->getStatements()->delete($this->id),
                'bindValue' => $this->bindValues($state, $this->args),
                'execute'   => $this->bindExecute($client, $state, $this->args),
                default => new SQLite3Exception("Invalid SQLite3 statement operation {$this->operation}")
            };
        }
        return new SQLite3Exception("Could not find statement {$this->id}");
    }
}
