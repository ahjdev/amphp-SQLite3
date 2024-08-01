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

use Amp\Sql\Common\SqlNestableTransactionExecutor;
use Amp\SQLite3\SQLite3Executor;
use Amp\SQLite3\SQLite3Result;
use Amp\SQLite3\SQLite3Statement;

/**
 * @internal
 * @implements SqlNestableTransactionExecutor<SQLite3Result, SQLite3Statement>
 */
final class SQLite3NestableExecutor implements SQLite3Executor, SqlNestableTransactionExecutor
{
    public function __construct(private readonly ConnectionProcessor $processor)
    {
    }

    public function isClosed(): bool
    {
        return $this->processor->isClosed();
    }

    /**
     * @return int Timestamp of the last time this connection was used.
     */
    public function getLastUsedAt(): int
    {
        return $this->processor->getLastUsedAt();
    }

    public function close(): void
    {
        // Send close command if connection is not already in a closed or closing state
        if (!$this->processor->isClosed()) {
            $this->processor->close();
        }
    }

    public function onClose(\Closure $onClose): void
    {
        $this->processor->onClose($onClose);
    }

    public function query(string $sql): SQLite3Result
    {
        return $this->processor->query($sql)->await();
    }

    public function prepare(string $sql): SQLite3Statement
    {
        return $this->processor->prepare($sql)->await();
    }

    public function execute(string $sql, array $params = []): SQLite3Result
    {
        $statement = $this->prepare($sql);
        return $statement->execute($params);
    }

    public function commit(): void
    {
        $this->query("COMMIT");
    }

    public function rollback(): void
    {
        $this->query("ROLLBACK");
    }

    public function createSavepoint(string $identifier): void
    {
        $this->query(\sprintf("SAVEPOINT %s", $identifier));
    }

    public function rollbackTo(string $identifier): void
    {
        $this->query(\sprintf("ROLLBACK TO %s", $identifier));
    }

    public function releaseSavepoint(string $identifier): void
    {
        $this->query(\sprintf("RELEASE %s", $identifier));
    }
}
