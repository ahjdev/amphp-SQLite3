<?php declare(strict_types=1);

namespace Amp\SQLite3;

use Amp\Sql\SqlConnection;
use Amp\Sql\SqlTransactionIsolation;

interface SQLite3Connection extends SQLite3Link
{
    /**
     * @return SQLite3Config The configuration used to create this connection.
     */
    public function getConfig(): SQLite3Config;

    /**
     * @return SqlTransactionIsolation Current transaction isolation used when beginning transactions on this connection.
     */
    public function getTransactionIsolation(): SqlTransactionIsolation;

    /**
     * Sets the transaction isolation level for transactions began on this link.
     *
     * @see SqlLink::beginTransaction()
     */
    public function setTransactionIsolation(SqlTransactionIsolation $isolation): void;
}
