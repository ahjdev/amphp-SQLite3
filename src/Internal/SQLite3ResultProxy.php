<?php declare(strict_types=1);

/**
 * This file is part of Reymon.
 * Reymon is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 * Reymon is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * @author    AhJ <AmirHosseinJafari8228@gmail.com>
 * @copyright 2023-2024 AhJ <AmirHosseinJafari8228@gmail.com>
 * @license   https://choosealicense.com/licenses/gpl-3.0/ GPLv3
 */

namespace Amp\SQLite3\Internal;

use Amp\DeferredFuture;
use Amp\Pipeline\Queue;
use Amp\SQLite3\SQLite3Result;
use Amp\Pipeline\ConcurrentIterator;

/**
 * @internal
 * @psalm-import-type TFieldType from SQLite3Result
 */
final class SQLite3ResultProxy
{
    /** @var Queue<list<TFieldType>> */
    private readonly Queue $rowQueue;

    /** @var ConcurrentIterator<list<TFieldType>> */
    public readonly ConcurrentIterator $rowIterator;

    // /** @var DeferredFuture<SQLite3Result|null>|null */
    // public ?DeferredFuture $next = null;

    public function __construct(
        \SQLite3Result $result,
        public readonly int $columnCount = 0,
        public readonly ?int $affectedRows = null,
        public readonly ?int $insertId = null,
    ) {
        $this->rowQueue = new Queue();
        $this->rowIterator = $this->rowQueue->iterate();
        while ($fetch = $result->fetchArray(SQLITE3_ASSOC)) {
            $this->rowQueue->pushAsync($fetch);
        }
        $this->rowQueue->complete();
    }

    public function __serialize()
    {
        return [
            'columnCount'  => $this->columnCount,
            'affectedRows' => $this->affectedRows,
            'insertId'     => $this->insertId,
            'result'       => iterator_to_array($this->rowIterator),
        ];
    }

    public function __unserialize($data)
    {
        $this->rowQueue     = new Queue();
        $this->rowIterator  = $this->rowQueue->iterate();
        $this->columnCount  = $data['columnCount'];
        $this->affectedRows = $data['affectedRows'];
        $this->insertId     = $data['insertId'];
        array_map($this->pushRow(...), $data['result']);
        $this->rowQueue->complete();
    }

    /**
     * @param list<TFieldType> $row
     */
    public function pushRow(array $row): void
    {
        $this->rowQueue->push($row);
    }

    public function complete(): void
    {
        if (!$this->rowQueue->isComplete()) {
            $this->rowQueue->complete();
        }
    }

    public function error(\Throwable $e): void
    {
        if (!$this->rowQueue->isComplete()) {
            $this->rowQueue->error($e);
        }
    }
}
