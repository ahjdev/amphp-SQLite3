<?php declare(strict_types=1);

namespace Amp\SQLite3;

use Amp\Cancellation;
use Amp\DeferredFuture;
use Amp\Sql\SqlTransactionIsolation;
use Amp\Sync\ChannelException;
use Revolt\EventLoop;

final class SQLite3WorkerConnection implements SQLite3Connection
{
    private SqlTransactionIsolation $transactionIsolation = SQLite3TransactionIsolationLevel::Deferred;

    private ?DeferredFuture $busy = null;

    /** @var \Closure():void Function used to release connection after a transaction has completed. */
    private readonly \Closure $release;

    public static function connect(SQLite3Config $config, ?Cancellation $cancellation = null): self
    {
        try {
            $processor = new Internal\ConnectionProcessor($config, $cancellation);
        } catch (ChannelException $exception) {
            throw new SQLite3Exception(
                'Connecting to the SQLite3 server failed: ' . $exception->getMessage(),
                previous: $exception,
            );
        }
        return new self($processor);
    }

    private function __construct(private readonly Internal\ConnectionProcessor $processor)
    {
        $busy = &$this->busy;
        $this->release = static function () use (&$busy): void {
            $busy?->complete();
            $busy = null;
        };
    }

    public function getConfig(): SQLite3Config
    {
        return $this->processor->getConfig();
    }

    public function getTransactionIsolation(): SqlTransactionIsolation
    {
        return $this->transactionIsolation;
    }

    public function setTransactionIsolation(SqlTransactionIsolation $isolation): void
    {
        $this->transactionIsolation = $isolation;
    }

    /**
     * @return bool False if the connection has been closed.
     */
    public function isClosed(): bool
    {
        return $this->processor->isClosed();
    }

    /**
     * @return int Timestamp of the last time this connection was used.
     */
    public function getLastUsedAt(): int
    {
        return $this->processor->getLastUsedAt();
    }

    public function close(): void
    {
        // Send close command if connection is not already in a closed or closing state
        if (!$this->processor->isClosed()) {
            $this->processor->close();
        }
    }

    public function onClose(\Closure $onClose): void
    {
        $this->processor->onClose($onClose);
    }

    public function query(string $sql): SQLite3Result
    {
        while ($this->busy) {
            $this->busy->getFuture()->await();
        }
        return $this->processor->query($sql)->await();
    }

    public function beginTransaction(): SQLite3Transaction
    {
        while ($this->busy) {
            $this->busy->getFuture()->await();
        }

        $this->busy = $deferred = new DeferredFuture();

        $sql = \sprintf("BEGIN %s", $this->transactionIsolation->toSql());

        try {
            $this->processor->query($sql)->await();
        } catch (\Throwable $exception) {
            $this->busy = null;
            $deferred->complete();
            throw $exception;
        }
        $executor = new Internal\SQLite3NestableExecutor($this->processor);
        return new Internal\SQLite3ConnectionTransaction($executor, $this->release, $this->transactionIsolation);
    }

    public function prepare(string $sql): SQLite3Statement
    {
        while ($this->busy) {
            $this->busy->getFuture()->await();
        }
        return $this->processor->prepare($sql)->await();
    }

    public function execute(string $sql, array $params = []): SQLite3Result
    {
        $statement = $this->prepare($sql);
        return $statement->execute($params);
    }

    public function __destruct()
    {
        EventLoop::queue($this->close(...));
    }
}
