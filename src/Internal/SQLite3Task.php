<?php declare(strict_types=1);

namespace Amp\SQLite3\Internal;

use Amp\Cancellation;
use Amp\Sync\Channel;
use Amp\Cache\LocalCache;
use Amp\Cache\CacheException;
use Amp\Parallel\Worker\Task;
use Amp\SQLite3\SQLite3Exception;
use Amp\ByteStream\ClosedException;
use Amp\ByteStream\StreamException;
use Amp\File\Driver\BlockingFilesystemDriver;
use Amp\SQLite3\Driver\BlockingSQLite3Driver;
use Amp\SQLite3\Driver\BlockingSQLite3Result;
use Amp\SQLite3\Driver\BlockingSQLite3Statement;

/**
 * @codeCoverageIgnore
 * @internal
 * @implements Task<mixed, never, never>
 */
final class SQLite3Task implements Task
{
    private static ?LocalCache $resultCache = null;
    private static ?LocalCache $stmtCache   = null;

    private static ?BlockingSQLite3Driver $driver = null;

    /**
     * @param string|int|null $id Task ID.
     *
     * @throws \Error
     */
    public function __construct(
        private readonly string $operation,
        private readonly array $args = [],
        private readonly string|int|null $id = null,
    ) {
        if ($operation === '') {
            throw new \Error('Operation must be a non-empty string');
        }
    }

    /**
     * @throws SQLite3Exception
     * @throws CacheException
     * @throws ClosedException
     * @throws StreamException
     */
    public function run(Channel $channel, Cancellation $cancellation): mixed
    {
        $stmtCache   = self::$stmtCache   ??= new LocalCache();
        $resultCache = self::$resultCache ??= new LocalCache();
        $driver = self::$driver;

        switch ($this->operation)
        {
            case 'init':
                $config = $this->args[0];
                self::$driver = new BlockingSQLite3Driver(
                    $config->getFileName(), $config->getFlags(), $config->getEncryptionKey()
                );
                return null;

            case 'query':
                $handle = $driver->query(...$this->args);
                $id = $handle->getId();

                if ($handle->getColumnCount()) {
                    $resultCache->set($id, $handle);
                    $handle->onClose(static fn (): ?bool => $resultCache->delete($id));
                }

                return $this->createResult($handle);

            case 'prepare':
                $handle = $driver->prepare(...$this->args);
                $id = $handle->getId();
                $handle->onClose(static fn (): ?bool => $stmtCache->delete($id));
                $stmtCache->set($id, $handle);
                return [$id];

            case 'execute':
                if ($this->id === null) {
                    return $driver->execute(...$this->args);
                } else {
                    $handle = $this->getStatement($this->id);
                    $result = $handle->execute(...$this->args);
                    return $this->createResult($result);
                }

            case 'fetchRow':
            case 'closeResult':
            case 'columnName':
            case 'columnType':
                $handle = $this->getResult($this->id);
                return $handle->{$this->operation}(...$this->args);

            case 'getQuery':
                $handle = $this->getStatement($this->id);
                return $handle->getQuery();
        }

        throw new \Error("Invalid operation - " . $this->operation);
    }

    private function getResult(?string $id): BlockingSQLite3Result
    {
        if ($id === null) {
            throw new SQLite3Exception("No SQLite3Result ID provided");
        }

        $handle = self::$resultCache->get($this->id);

        if ($handle === null) {
            throw new SQLite3Exception(\sprintf(
                "No SQLite3Result handle with the ID %s has been opened on the worker",
                $this->id
            ));
        }

        if (!$handle instanceof BlockingSQLite3Result) {
            throw new SQLite3Exception("SQLite3Result storage found in inconsistent state");
        }

        return $handle;
    }

    private function getStatement(?string $id): BlockingSQLite3Statement
    {
        if ($id === null) {
            throw new SQLite3Exception("No SQLite3Stmt ID provided");
        }

        $handle = self::$stmtCache->get($this->id);

        if ($handle === null) {
            throw new SQLite3Exception(\sprintf(
                "No SQLite3Stmt handle with the ID %s has been opened on the worker",
                $this->id
            ));
        }

        if (!$handle instanceof BlockingSQLite3Statement) {
            throw new SQLite3Exception("SQLite3Stmt storage found in inconsistent state");
        }

        return $handle;
    }

    private function createResult(BlockingSQLite3Result $result)
    {
        return [
            $result->getId(),
            $result->getLastInsertId(),
            $result->getRowCount(),
            $result->getColumnCount(),
        ];
    }
}
