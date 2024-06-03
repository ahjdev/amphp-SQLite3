<?php declare(strict_types=1);

namespace Amp\SQLite3\Internal;

use Amp\ByteStream\ResourceStream;
use Amp\DeferredFuture;
use Amp\Future;
use Amp\Mysql\MysqlColumnDefinition;
use Amp\Mysql\MysqlConfig;
use Amp\Mysql\MysqlDataType;
use Amp\Mysql\MysqlResult;
use Amp\Parser\Parser;
use Amp\Socket\Socket;
use Amp\Sql\SqlConnectionException;
use Amp\Sql\SqlQueryError;
use Amp\Sql\SqlTransientResource;

/**
 * @internal
 */
final class ConnectionProcessor implements SqlTransientResource
{
    private MysqlConfig $config;

    /** @var \SplQueue<DeferredFuture> */
    private readonly \SplQueue $deferreds;

    /** @var \SplQueue<\Closure():void> */
    private readonly \SplQueue $onReady;

    private ?MysqlResultProxy $result = null;

    private int $lastUsedAt;

    public function __construct(MysqlConfig $config)
    {
        $this->config = $config;
        $this->lastUsedAt = \time();
        $this->deferreds = new \SplQueue();
        $this->onReady = new \SplQueue();
    }

    public function isClosed(): bool
    {
    }

    public function onClose(\Closure $onClose): void
    {
    }

    private function enqueueDeferred(DeferredFuture $deferred): void
    {
        \assert(!$this->socket->isClosed(), "The connection has been closed");
        $this->deferreds->push($deferred);
    }

    private function dequeueDeferred(): DeferredFuture
    {
        \assert(!$this->deferreds->isEmpty(), 'Pending deferred not found when shifting from pending queue');
        return $this->deferreds->shift();
    }

    /**
     * @param \Closure():void $callback
     */
    private function appendTask(\Closure $callback): void
    {
        if ($this->packetCallback
            || $this->parseCallback
            || !$this->onReady->isEmpty()
            || !$this->deferreds->isEmpty()
        ) {
            $this->onReady->push($callback);
        } else {
            $callback();
        }
    }

    public function getConfig(): MysqlConfig
    {
        return $this->config;
    }

    public function getLastUsedAt(): int
    {
        return $this->lastUsedAt;
    }

    protected function startCommand(\Closure $callback): Future
    {
        if ($this->isClosed()) {
            throw new \Error("The connection has been closed");
        }

        $deferred = new DeferredFuture;
        $this->appendTask(function () use ($callback, $deferred) {
            $this->enqueueDeferred($deferred);
            $callback();
        });
        return $deferred->getFuture();
    }

    /**
     * @return Future<MysqlResult>
     */
    public function query(string $query): Future
    {
        return $this->startCommand(function () use ($query): void {
            $this->query = $query;
            $this->parseCallback = $this->handleQuery(...);
            $this->write("\x03$query");
        });
    }

    /**
     * @return Future<MysqlConnectionStatement>
     */
    public function prepare(string $query): Future
    {
        return $this->startCommand(function () use ($query): void {
            $this->query = $query;
            $this->parseCallback = $this->handlePrepare(...);
            $this->write("\x16$query");
        });
    }

    public function bindParam(int $stmtId, int $paramId, string $data): void
    {
        $this->appendTask(function () use ($payload): void {
            $this->write(\implode($payload));
        });
    }

    /**
     * @param list<MysqlColumnDefinition> $params
     * @param array<int, string> $prebound
     * @param array<int, mixed> $data
     */
    public function execute(int $stmtId, string $query, array $params, array $prebound, array $data = []): Future
    {
        $deferred = new DeferredFuture;
        $this->appendTask(function () use ($stmtId, $query, $params, $prebound, $data, $deferred): void {
            $this->enqueueDeferred($deferred);
            $this->write(\implode($payload));
            // apparently LOAD DATA LOCAL INFILE requests are not supported via prepared statements
            $this->packetCallback = $this->handleExecute(...);
        });
        return $deferred->getFuture(); // do not use $this->startCommand(), that might unexpectedly reset the seqId!
    }

    public function closeStmt(int $stmtId): void
    {
        $payload = "\x19" . MysqlDataType::encodeInt32($stmtId);
        $this->appendTask(function () use ($payload): void {
            if ($this->connectionState === ConnectionState::Ready) {
                $this->write($payload);
            }
            $this->ready();
        });
    }

    /** @see 14.7.8 COM_STMT_RESET */
    public function resetStmt(int $stmtId): Future
    {
        $deferred = new DeferredFuture;
        $this->appendTask(function () use ($payload, $deferred): void {
            $this->resetIds();
            $this->enqueueDeferred($deferred);
            $this->write($payload);
        });
        return $deferred->getFuture();
    }

    /** @see 14.8.4 COM_STMT_FETCH */
    public function fetchStmt(int $stmtId): Future
    {
        $deferred = new DeferredFuture;
        $this->appendTask(function () use ($payload, $deferred): void {
            $this->resetIds();
            $this->enqueueDeferred($deferred);
            $this->write($payload);
        });
        return $deferred->getFuture();
    }

    /** @see 14.1.3.2 ERR-Packet */
    private function handleError(string $packet): void
    {
        if ($connecting) {
            // connection failure
            $this->free(new SqlConnectionException(\sprintf(
                'Could not connect to %s: %s',
                $this->config->getConnectionString(),
                $this->metadata->errorMsg,
            )));
            return;
        }

        if ($this->result === null && $this->deferreds->isEmpty()) {
            // connection killed without pending query or active result
            $this->free(new SqlConnectionException('Connection closed after receiving an unexpected error packet'));
            return;
        }

        $deferred = $this->result ?? $this->dequeueDeferred();

        // normal error
        $exception = new SqlQueryError(\sprintf(
            'MySQL error (%d): %s %s',
            $this->metadata->errorCode,
            $this->metadata->errorState ?? 'Unknown state',
            $this->metadata->errorMsg,
        ), $this->query ?? '');

        $this->result = null;
        $this->query = null;
        $this->named = [];

        $deferred->error($exception);
    }

    private function handleOk(string $packet): void
    {
        $this->parseOk($packet);
        $this->dequeueDeferred()->complete(
            new MysqlCommandResult($this->metadata->affectedRows, $this->metadata->insertId),
        );
    }

    private function handleQuery(string $packet): void
    {
        switch (\ord($packet)) {
            case self::OK_PACKET:
                $this->parseOk($packet);

                if ($this->metadata->statusFlags & self::SERVER_MORE_RESULTS_EXISTS) {
                    $this->result = new MysqlResultProxy(
                        affectedRows: $this->metadata->affectedRows,
                        insertId: $this->metadata->insertId
                    );
                    $this->result->markDefinitionsFetched();
                    $this->dequeueDeferred()->complete(new MysqlConnectionResult($this->result));
                    $this->successfulResultFetch();
                } else {
                    $this->parseCallback = null;
                    $this->dequeueDeferred()->complete(new MysqlCommandResult(
                        $this->metadata->affectedRows,
                        $this->metadata->insertId
                    ));
                    $this->ready();
                }
                return;
            case self::LOCAL_INFILE_REQUEST:
                if ($this->config->isLocalInfileEnabled()) {
                    $this->handleLocalInfileRequest($packet);
                } else {
                    $this->dequeueDeferred()->error(new SqlConnectionException("Unexpected LOCAL_INFILE_REQUEST packet"));
                }
                return;
            case self::ERR_PACKET:
                $this->handleError($packet);
                return;
        }

        $this->result = new MysqlResultProxy(MysqlDataType::decodeUnsigned($packet));
        $this->dequeueDeferred()->complete(new MysqlConnectionResult($this->result));
    }

    /** @see 14.7.1 Binary Protocol Resultset */
    private function handleExecute(string $packet): void
    {
        $this->result = new MysqlResultProxy(\ord($packet));
        $this->dequeueDeferred()->complete(new MysqlConnectionResult($this->result));
    }

    /** @see 14.7.4.1 COM_STMT_PREPARE Response */
    private function handlePrepare(string $packet): void
    {
        switch (\ord($packet)) {
            case self::OK_PACKET:
                break;
            case self::ERR_PACKET:
                $this->handleError($packet);
                return;
            default:
                throw new SqlConnectionException("Unexpected value for first byte of COM_STMT_PREPARE Response");
        }

        $offset = 1;

        $stmtId = MysqlDataType::decodeUnsigned32($packet, $offset);
        $columns = MysqlDataType::decodeUnsigned16($packet, $offset);
        $params = MysqlDataType::decodeUnsigned16($packet, $offset);

        $offset += 1; // filler

        $this->metadata->warnings = MysqlDataType::decodeUnsigned16($packet, $offset);

        $this->result = new MysqlResultProxy($columns, $params);
        $this->refcount++;
        \assert($this->query !== null, 'Invalid value for connection query');
        $this->dequeueDeferred()->complete(new MysqlConnectionStatement($this, $this->query, $stmtId, $this->named, $this->result));
        $this->named = [];
        if ($params) {
            $this->parseCallback = $this->prepareParams(...);
        } else {
            $this->prepareParams($packet);
        }
    }

    public function sendClose(): Future
    {
        return $this->startCommand(function (): void {
        });
    }

    public function close(): void
    {
    }
}
