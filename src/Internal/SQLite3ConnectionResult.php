<?php declare(strict_types=1);

namespace Amp\SQLite3\Internal;

use Amp\Future;
use Revolt\EventLoop;
use Amp\DeferredFuture;
use Amp\SQLite3\SQLite3Result;
use SQLite3Result as SqliteResult;

use function Amp\async;

/**
 * @internal
 * @psalm-import-type TRowType from SQLite3Result
 * @implements \IteratorAggregate<int, TRowType>
 */
final class SQLite3ConnectionResult implements SQLite3Result, \IteratorAggregate
{
    private readonly \Generator $generator;

    private array $result;

    private ?Future $nextResult = null;

    public function __construct(
        SqliteResult $SQLite3Result,
        private readonly int $affectedRows,
        private readonly int $columnCount,
        private readonly int $insertId
    ) {
        while ($result = $SQLite3Result->fetchArray(SQLITE3_ASSOC)) {
            $this->result[] = $result;
        }
        $this->generator = $this->iterate();
    }

    private function iterate(): \Generator
    {
        foreach ($this->result as $name => $value) {
            yield $name => $value;
        }
    }

    public function __destruct()
    {
        EventLoop::queue(self::dispose(...), $this->generator);
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
        // Using a Generator to keep a reference to $this.
        yield from $this->generator;
    }

    public function fetchRow(): ?array
    {
        if (!$this->generator->valid()) {
            return null;
        }

        $current = $this->generator->current();
        $this->generator->next();
        return $current;
    }

    public function getNextResult(): ?SQLite3Result
    {
        $this->nextResult ??= async(function (): ?SQLite3Result {
            self::dispose($this->generator);
            if ($this->generator->valid()) {
                return $this->generator->next();
            }
            return null;
        });
        return $this->nextResult->await();
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
        return $this->insertId;
    }
}
