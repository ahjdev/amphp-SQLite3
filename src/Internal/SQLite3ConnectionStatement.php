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

use Amp\DeferredFuture;
use Amp\SQLite3\SQLite3Result;
use Amp\SQLite3\SQLite3Statement;

final class SQLite3ConnectionStatement implements SQLite3Statement
{
    private int $lastUsedAt;
    private int $totalParamCount;
    private string $statementId;
    private readonly DeferredFuture $onClose;

    public function __construct(
        private ?ConnectionProcessor $processor,
        SQLite3ChannelStatement $statement,
    ) {
        $this->onClose         = new DeferredFuture();
        $this->lastUsedAt      = \time();
        $this->statementId     = $statement->uniqid;
        $this->totalParamCount = $statement->totalParamCount;
    }

    public function isClosed(): bool
    {
        return !$this->processor || $this->processor->isClosed();
    }

    public function close(): void
    {
        if (!$this->isClosed()) {
            try {
                $this->processor->closeStmt($this->statementId);
            } finally {
                $this->onClose->complete();
            }
            $this->processor = null;
        }
    }

    public function onClose(\Closure $onClose): void
    {
        $this->onClose->getFuture()->finally($onClose);
    }

    public function bind(int|string $param, string $data): void
    {
        $this->processor->bindParam($this->statementId, $param, $data);
    }

    public function execute(array $params = []): SQLite3Result
    {
        $this->lastUsedAt = \time();
        return $this->processor->executeStmt($this->statementId, $params)->await();
    }

    public function getQuery(): string
    {
        return $this->processor->getQueryStmt($this->statementId)->await();
    }

    public function getTotalParamCount(): int
    {
        return $this->totalParamCount;
    }

    public function reset(): bool
    {
        return $this->processor->resetStmt($this->statementId)->await();
    }

    public function getLastUsedAt(): int
    {
        return $this->lastUsedAt;
    }

    public function __destruct()
    {
        $this->close();
    }
}
