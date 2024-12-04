<?php declare(strict_types=1);

namespace Amp\SQLite3;

use Amp\Sql\SqlLink;

/**
 * @extends SqlLink<SQLite3Result, SQLite3Statement, SQLite3Transaction>
 */
interface SQLite3Link extends SQLite3Executor, SqlLink
{
    /**
     * @return SQLite3Transaction Transaction object specific to this library.
     */
    public function beginTransaction(): SQLite3Transaction;
}
