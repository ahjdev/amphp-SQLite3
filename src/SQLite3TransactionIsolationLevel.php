<?php declare(strict_types=1);

namespace Amp\SQLite3;

use Amp\Sql\SqlTransactionIsolation;

enum SQLite3TransactionIsolationLevel: int implements SqlTransactionIsolation
{
    case Deferred = 0;
    case Immediate = 1;
    case Exclusive = 2;

    public function getLabel(): string
    {
        return $this->name;
    }

    public function toSql(): string
    {
        return match ($this) {
            self::Deferred  => 'DEFERRED',
            self::Immediate => 'IMMEDIATE',
            self::Exclusive => 'EXCLUSIVE READ',
        };
    }
}
