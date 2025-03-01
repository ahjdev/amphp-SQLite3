<?php declare(strict_types=1);

namespace Amp\SQLite3\Driver;

use Amp\DeferredFuture;
use Amp\SQLite3\SQLite3Result;
use Amp\SQLite3\SQLite3Exception;
use Amp\ByteStream\StreamException;

/**
 * @implements \IteratorAggregate<int, string>
 */
final class BlockingSQLite3Result implements SQLite3Result, \IteratorAggregate
{
    private readonly DeferredFuture $onClose;

    /**
     * number of columns returned by the query if applicable or null if the number of columns is unknown or not applicable to the query
     */
    private int $columnCount;

    /**
     * @param ?\SQLite3Result $handle An open SQLite3Result descriptor.
     * @param ?int $lastInsertId Insert ID of the last auto increment row if applicable to the result or null if no ID is available.
     * @param ?int $affectedRows Number of rows affected or returned by the query if applicable or null if the number of rows is unknown or not applicable to the query.
     */
    public function __construct(
        private ?\SQLite3Result $handle,
        private readonly string $id,
        private readonly ?int $lastInsertId,
        private readonly ?int $affectedRows,
    ) {
        $this->handle = $handle;
        $this->columnCount = $handle->numColumns();
        $this->onClose = new DeferredFuture;
    }

    public function __destruct()
    {
        if ($this->handle !== null) {
            $this->handle->finalize();
            $this->handle = null;
        }

        if (!$this->onClose->isComplete()) {
            $this->onClose->complete();
        }
    }

    public function columnName(int $column): string
    {
        if ($this->handle === null) {
            throw new SQLite3Exception("The SQLite3Result object has been closed");
        }

        try {
            \set_error_handler(function (int $type, string $message) use ($column): never {
                throw new SQLite3Exception("Failed reading name of the column '{$column}': {$message}");
            });

            $columnName = $this->handle->columnName($column);

            if ($columnName === false) {
                throw new SQLite3Exception("Column '{$column}' does not exist.");
            }

            return $columnName;
        } finally {
            \restore_error_handler();
        }
    }

    public function columnType(int $column): int
    {
        if ($this->handle === null) {
            throw new SQLite3Exception("The SQLite3Result object has been closed");
        }

        try {
            \set_error_handler(function (int $type, string $message) use ($column): never {
                throw new SQLite3Exception("Failed reading type of the column '{$column}': {$message}");
            });

            $columnType = $this->handle->columnType($column);

            if ($columnType === false) {
                throw new SQLite3Exception("Column '{$column}' does not exist.");
            }

            return $columnType;
        } finally {
            \restore_error_handler();
        }
    }

    public function closeResult(): void
    {
        if ($this->handle === null) {
            return;
        }

        $handle = $this->handle;
        $this->handle = null;

        if (!$this->onClose->isComplete()) {
            $this->onClose->complete();
        }

        try {
            \set_error_handler(function (int $type, string $message): never {
                throw new SQLite3Exception("Failed closing SQLite3Result: {$message}");
            });

            if ($handle->finalize()) {
                return;
            }

            throw new SQLite3Exception("Failed closing SQLite3Result");
        } finally {
            \restore_error_handler();
        }
    }

    public function isClosed(): bool
    {
        return $this->handle === null;
    }

    public function onClose(\Closure $onClose): void
    {
        $this->onClose->getFuture()->finally($onClose);
    }

    public function getId(): string
    {
        return $this->id;
    }

    public function getNextResult(): ?self
    {
        return null;
    }

    public function getLastInsertId(): ?int
    {
        return $this->lastInsertId;
    }

    public function fetchRow(): ?array
    {
        return $this->handle->fetchArray(\SQLITE3_ASSOC) ?: null;
    }

    public function getRowCount(): ?int
    {
        return $this->affectedRows;
    }

    public function getColumnCount(): ?int
    {
        return $this->columnCount;
    }

    public function getIterator(): \Traversable
    {
        while (!$fetch = $this->fetchRow()) {
            yield $fetch; // todo
        }
    }
}
