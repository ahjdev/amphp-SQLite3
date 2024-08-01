<?php declare(strict_types=1);

namespace Amp\SQLite3\Internal;

use Amp\Future;
use Amp\SQLite3\SQLite3Result;
use Revolt\EventLoop;

/**
 * @internal
 * @psalm-import-type TRowType from SQLite3Result
 * @implements \IteratorAggregate<int, TRowType>
 */
final class SQLite3ConnectionResult implements SQLite3Result, \IteratorAggregate
{
    private ?Future $nextResult = null;
    private string $uniqid;
    private int $columnCount;
    private ?int $affectedRows;
    private ?int $lastInsertId;

    public function __construct(private ?ConnectionProcessor $processor, SQLite3ChannelResult $result)
    {
        $this->uniqid = $result->uniqid;
        $this->columnCount  = $result->columnCount;
        $this->affectedRows = $result->affectedRows;
        $this->lastInsertId = $result->lastInsertId;
    }

    public function __destruct()
    {
        EventLoop::queue($this->processor->closeResult(...), $this->uniqid);
    }

    private static function dispose(\Generator $generator): void
    {
        try {
            // Discard remaining rows in the result set.
            while ($generator->valid()) {
                $generator->next();
            }
        } catch (\Throwable) {
            // Ignore errors while discarding result.
        }
    }

    public function getIterator(): \Traversable
    {
        while ($future = $this->processor->getNextResult($this->uniqid)->await()) {
            /** @var SQLite3ChannelResutlArray */
            yield $future->result ?? [];
        }
    }

    public function fetchRow(): ?array
    {
        // if (!$this->generator->valid()) {
        //     return null;
        // }

        // $current = $this->result->result;
        // $this->generator->next();
        // return $current;
        return null;
    }

    public function getNextResult(): ?SQLite3Result
    {
        return null;
    }

    public function getRowCount(): ?int
    {
        return $this->affectedRows;
    }

    public function getColumnCount(): int
    {
        return $this->columnCount;
    }

    public function getLastInsertId(): ?int
    {
        return $this->lastInsertId;
    }
}
