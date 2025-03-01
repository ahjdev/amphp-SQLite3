<?php declare(strict_types=1);

namespace Amp\SQLite3;

use Amp\SQLite3\SQLite3Result;
use Amp\SQLite3\SQLite3Statement;

interface SQLite3Driver
{
    public function execute(string $query, array $params = []): SQLite3Result;
    public function query(string $query): SQLite3Result;
    public function prepare(string $query): SQLite3Statement;
}
