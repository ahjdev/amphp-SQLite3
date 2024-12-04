<?php declare(strict_types=1);

namespace Amp\SQLite3;

use Amp\Sql\SqlTransaction;

/**
 * Note that notifications sent during a transaction are not delivered until the transaction has been committed.
 *
 * @extends SqlTransaction<SQLite3Result, SQLite3Statement, SQLite3Transaction>
 */
interface SQLite3Transaction extends SQLite3Link, SqlTransaction
{
}
