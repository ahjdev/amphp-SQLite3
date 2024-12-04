<?php declare(strict_types=1);

namespace Amp\SQLite3;

use Amp\Sql\SqlResult;

/**
 * Recursive template types currently not supported, list<mixed> should be list<TFieldType>.
 * @psalm-type TFieldType = list<mixed>|scalar|null
 * @psalm-type TRowType = array<string, TFieldType>
 * @extends SqlResult<TFieldType>
 */
interface SQLite3Result extends SqlResult
{
    /**
     * Changes return type to this library's Result type.
     */
    public function getNextResult(): ?self;

    /**
     * @return int|null Insert ID of the last auto increment row if applicable to the result or null if no ID
     *                  is available.
     */
    public function getLastInsertId(): ?int;
}
