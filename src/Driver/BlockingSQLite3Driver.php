<?php declare(strict_types=1);

namespace Amp\SQLite3\Driver;

use Amp\SQLite3\SQLite3Driver;
use Amp\SQLite3\SQLite3Exception;

final class BlockingSQLite3Driver implements SQLite3Driver
{
    private readonly ?\SQLite3 $SQLite3;
    private static string $nextStmtId = 'a';
    private static string $nextResultId = 'a';

    public function __construct(
        string $filename,
        int $flags = SQLITE3_OPEN_READWRITE | SQLITE3_OPEN_CREATE,
        string $encryptionKey = ""
    ) {
        $this->SQLite3 = new \SQLite3($filename, $flags, $encryptionKey);
    }

    public function getLastError(): string
    {
        return $this->SQLite3->lastErrorMsg();
    }

    public function createResult(\SQLite3Result $handle): BlockingSQLite3Result
    {
        $lastInsertId = $this->SQLite3->lastInsertRowID() ?: 0;
        $affectedRows = $this->SQLite3->changes();

        return new BlockingSQLite3Result($handle, ++self::$nextResultId, $lastInsertId, $affectedRows);
    }

    public function query(string $query): BlockingSQLite3Result
    {
        try {
            \set_error_handler(static function (int $type, string $message): never {
                throw new SQLite3Exception("Failed to execute query: {$message}");
            });

            if (!$handle = $this->SQLite3->query($query)) {
                throw new SQLite3Exception("Failed to execute query: " . $this->getLastError());
            }

            return $this->createResult($handle);
        } finally {
            \restore_error_handler();
        }
    }

    public function prepare(string $query): BlockingSQLite3Statement
    {
        try {
            \set_error_handler(static function (int $type, string $message): never {
                throw new SQLite3Exception("Failed to prepare query: {$message}");
            });

            if (!$handle = $this->SQLite3->prepare($query)) {
                throw new SQLite3Exception("Failed to prepare query: " . $this->SQLite3->lastErrorMsg());
            }

            return new BlockingSQLite3Statement($this, $handle, ++self::$nextStmtId);
        } finally {
            \restore_error_handler();
        }
    }

    public function execute(string $query, array $params = []): BlockingSQLite3Result
    {
        $stmt = $this->prepare($query);
        return $stmt->execute($params);
    }

    public function __destruct()
    {
        $this->SQLite3->close();
    }
}
