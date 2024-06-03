<?php declare(strict_types=1);

namespace Amp\SQLite3;

function connect(
    string $filename,
    int $flags = SQLITE3_OPEN_READWRITE | SQLITE3_OPEN_CREATE,
    string $encryptionKey = ''
): SocketSQLite3Connection {
    $config = new SQLite3Config($filename, $flags, $encryptionKey);
    return SocketSQLite3Connection::connect($config);
}
