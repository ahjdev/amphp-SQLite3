<?php declare(strict_types=1);

namespace Amp\SQLite3;

final class SQLite3Config
{
    public function __construct(private string $filename, private int $flags = SQLITE3_OPEN_READWRITE | SQLITE3_OPEN_CREATE, private string $encryptionKey = '')
    {
        if (!\extension_loaded('sqlite3')) {
            throw new SQLite3Exception(__CLASS__ . " requires the sqlite3 extension");
        }
    }

    public function getFileName(): string
    {
        return $this->filename;
    }

    public function getFlags(): int
    {
        return $this->flags;
    }

    public function getEncryptionKey(): string
    {
        return $this->encryptionKey;
    }

    public function withFileName(string $filename): static
    {
        $new = clone $this;
        $new->filename = $filename;
        return $new;
    }

    public function withFlags(int $flags): static
    {
        $new = clone $this;
        $new->flags = $flags;
        return $new;
    }

    public function withEncryptionKey(string $encryptionKey): static
    {
        $new = clone $this;
        $new->encryptionKey = $encryptionKey;
        return $new;
    }
}
