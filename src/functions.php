<?php declare(strict_types=1);

namespace Amp\SQLite3;

function connect(
    string $filename,
    int $flags = SQLITE3_OPEN_READWRITE | SQLITE3_OPEN_CREATE,
    string $encryptionKey = ''
): SQLite3WorkerConnection {
    $config = new SQLite3Config($filename, $flags, $encryptionKey);
    return SQLite3WorkerConnection::connect($config);
}
