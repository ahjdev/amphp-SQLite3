<?php declare(strict_types=1);

namespace Amp\SQLite3;

use Amp\Sql\SqlExecutor;

/**
 * @extends SqlExecutor<SQLite3Result, SQLite3Statement>
 */
interface SQLite3Executor extends SqlExecutor
{
    /**
     * @return SQLite3Result Result object specific to this library.
     */
    public function query(string $sql): SQLite3Result;

    /**
     * @return SQLite3Statement Statement object specific to this library.
     */
    public function prepare(string $sql): SQLite3Statement;

    /**
     * @return SQLite3Result Result object specific to this library.
     */
    public function execute(string $sql, array $params = []): SQLite3Result;
}
