<?php declare(strict_types=1);

namespace Amp\SQLite3\Driver;

use Amp\DeferredFuture;
use Amp\SQLite3\SQLite3Result;
use Amp\SQLite3\SQLite3Exception;
use Amp\SQLite3\SQLite3Statement;
use Amp\ByteStream\StreamException;

final class BlockingSQLite3Statement implements SQLite3Statement
{
    private readonly DeferredFuture $onClose;

    private int $lastUsedAt;

    /**
     * @param ?\SQLite3Stmt $handle An open SQLite3Stmt descriptor.
     */
    public function __construct(
        private BlockingSQLite3Driver $driver,
        private ?\SQLite3Stmt $handle,
        private readonly string $id,
    ) {
        $this->handle = $handle;
        $this->lastUsedAt = \time();
        $this->onClose = new DeferredFuture;
    }

    public function __destruct()
    {
        if ($this->handle !== null) {
            $this->handle->close();
            $this->handle = null;
        }

        if (!$this->onClose->isComplete()) {
            $this->onClose->complete();
        }
    }

    public function execute(array $params = []): SQLite3Result
    {
        if ($this->handle === null) {
            throw new SQLite3Exception("The SQLite3Stmt object has been closed");
        }

        $this->lastUsedAt = \time();

        try {
            \set_error_handler(function (int $type, string $message): never {
                throw new SQLite3Exception("Failed execute statement: {$message}");
            });

            foreach ($params as $key => $value) {
                if (\is_string($value) && !\ctype_print($value)) {
                    $bind = $this->handle->bindValue($key, $value, SQLITE3_BLOB);
                } else {
                    $bind = $this->handle->bindValue($key, $value);
                }
            }

            if (!$handle = $this->handle->execute()) {
                throw new SQLite3Exception("Failed to execute statement: " . $this->driver->getLastError());
            }

            return $this->driver->createResult($handle);
        } finally {
            \restore_error_handler();
        }
    }

    public function getQuery(): string
    {
        if ($this->handle === null) {
            throw new SQLite3Exception("The SQLite3Stmt object has been closed");
        }

        try {
            \set_error_handler(function (int $type, string $message): never {
                throw new SQLite3Exception("Failed reading query: {$message}");
            });

            $query = $this->handle->getSQL(true);

            if ($query === false) {
                throw new SQLite3Exception("Failed reading query.");
            }

            return $query;
        } finally {
            \restore_error_handler();
        }
    }

    public function close(): void
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
                throw new StreamException("Failed closing SQLite3Stmt: {$message}");
            });

            if ($handle->close()) {
                return;
            }

            throw new StreamException("Failed closing SQLite3Stmt");
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

    public function getLastUsedAt(): int
    {
        return $this->lastUsedAt;
    }
}
