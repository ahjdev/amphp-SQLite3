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
use Amp\SQLite3\SQLite3Statement;

final class SQLite3ConnectionStatement implements SQLite3Statement
{
    
    private readonly int $totalParamCount;
    private readonly int $positionalParamCount;

    private array $named = [];

    /** @var array<string> */
    private array $prebound = [];

    private int $lastUsedAt;
    
    private readonly DeferredFuture $onClose;

    public function __construct(
        private readonly string $query,
        private readonly int $statementId,
        private readonly array $byNamed,
        private readonly MysqlResultProxy $result
    ) {
        $this->totalParamCount = $this->result->columnsToFetch;

        $this->onClose = new DeferredFuture();

        $positionalParamCount = $this->totalParamCount;
        foreach ($this->byNamed as $name => $ids) {
            foreach ($ids as $id) {
                $this->named[$id] = $name;
                $positionalParamCount--;
            }
        }

        $this->positionalParamCount = $positionalParamCount;

        $this->lastUsedAt = \time();
    }

    public function isClosed(): bool
    {
        return !$this->processor || $this->processor->isClosed();
    }

    public function close(): void
    {
        if ($this->processor) {
            self::shutdown($this->processor, $this->statementId, $this->onClose);
            $this->processor = null;
        }
    }

    public function onClose(\Closure $onClose): void
    {
        $this->onClose->getFuture()->finally($onClose);
    }

    public function onClose(\Closure $onClose): void
    {
        $this->onClose->getFuture()->finally($onClose);
    }

    public function bind(int|string $paramId, string $data): void
    {
        if (\is_int($paramId)) {
            if ($paramId >= $this->positionalParamCount || $paramId < 0) {
                throw new \Error("Parameter $paramId is not defined for this prepared statement");
            }
            $i = $paramId;
        } else {
            if (!isset($this->byNamed[$paramId])) {
                throw new \Error("Named parameter :$paramId is not defined for this prepared statement");
            }
            $array = $this->byNamed[$paramId];
            $i = \reset($array);
        }

        do {
            $realId = -1;
            while (isset($this->named[++$realId]) || $i-- > 0) {
                if (!\is_numeric($paramId) && isset($this->named[$realId]) && $this->named[$realId] === $paramId) {
                    break;
                }
            }

            $this->getProcessor()->bindParam($this->statementId, $realId, $data);
        } while (isset($array) && $i = \next($array));

        $prior = $this->prebound[$paramId] ?? '';
        $this->prebound[$paramId] = $prior . $data;
    }

    public function execute(array $params = []): MysqlResult
    {
        $this->lastUsedAt = \time();

        $prebound = $args = [];
        for ($unnamed = $i = 0; $i < $this->totalParamCount; $i++) {
            if (isset($this->named[$i])) {
                $name = $this->named[$i];
                if (\array_key_exists($name, $params)) {
                    $args[$i] = $params[$name];
                } elseif (!\array_key_exists($name, $this->prebound)) {
                    throw new \Error("Named parameter '$name' missing for executing prepared statement");
                } else {
                    $prebound[$i] = $this->prebound[$name];
                }
            } elseif (\array_key_exists($unnamed, $params)) {
                $args[$i] = $params[$unnamed];
                $unnamed++;
            } elseif (!\array_key_exists($unnamed, $this->prebound)) {
                throw new \Error("Parameter $unnamed missing for executing prepared statement");
            } else {
                $prebound[$i] = $this->prebound[$unnamed++];
            }
        }

        return $this->getProcessor()
            ->execute($this->statementId, $this->query, $this->result->params, $prebound, $args)
            ->await();
    }

    public function getQuery(): string
    {
        return $this->query;
    }

    public function reset(): void
    {
        $this->getProcessor()
            ->resetStmt($this->statementId)
            ->await();
    }

    public function getLastUsedAt(): int
    {
        return $this->lastUsedAt;
    }

    public function __destruct()
    {
        if ($this->processor) {
            EventLoop::queue(self::shutdown(...), $this->processor, $this->statementId, $this->onClose);
        }
    }

    private static function shutdown(ConnectionProcessor $processor, int $stmtId, DeferredFuture $onClose): void
    {
        try {
            $processor->closeStmt($stmtId);
            $processor->unreference();
        } finally {
            $onClose->complete();
        }
    }
}
