<?php declare(strict_types=1);

namespace Amp\SQLite3\Internal;

use Amp\Future;
use Revolt\EventLoop;
use function Amp\async;
use Amp\SQLite3\SQLite3Result;
use Amp\SQLite3\Internal\SQLite3Worker\SQLite3WorkerResult;

/**
 * @internal
 * @psalm-import-type TRowType from SQLite3Result
 * @implements \IteratorAggregate<int, TRowType>
 */
final class SQLite3ConnectionResult implements SQLite3Result, \IteratorAggregate
{
    private readonly SQLite3WorkerResult $result;

    private readonly \Generator $generator;

    private ?Future $nextResult = null;

    public function __construct(SQLite3WorkerResult $result)
    {
        $this->result = $result;
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
        return $this->result->getRowCount();
    }

    public function getColumnCount(): int
    {
        return $this->result->getColumnCount();
    }

    public function getLastInsertId(): ?int
    {
        return $this->result->getLastInsertId();
    }
}
