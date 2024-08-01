<?php declare(strict_types=1);

namespace Amp\SQLite3\Internal;

use Throwable;
use Amp\Future;
use Amp\Cancellation;
use Revolt\EventLoop;
use Amp\DeferredFuture;
use Amp\SQLite3\SQLite3Config;
use Amp\Parallel\Context\Context;
use Amp\Sql\SqlTransientResource;
use Amp\SQLite3\SQLite3Exception;
use Amp\SQLite3\SQLite3QueryError;
use Amp\Parallel\Context\StatusError;
use Amp\SQLite3\SQLite3ConnectionException;
use Amp\SQLite3\Internal\SQLite3Command\Query;
use Amp\SQLite3\Internal\SQLite3Command\Prepare;
use function Amp\Parallel\Context\contextFactory;
use Amp\SQLite3\Internal\SQLite3Command\ResultOperation;
use Amp\SQLite3\Internal\SQLite3Command\StatementOperation;

/**
 * @internal
 */
final class ConnectionProcessor implements SqlTransientResource
{
    /** @var \SplQueue<DeferredFuture> */
    private readonly \SplQueue $deferreds;

    /** @var \SplQueue<\Closure():void> */
    private readonly \SplQueue $onReady;

    private readonly DeferredFuture $onClose;

    private Context $context;

    private int $lastUsedAt;

    public function __construct(private SQLite3Config $config, ?Cancellation $cancellation = null)
    {
        $this->context = contextFactory()->start(__DIR__ . '/SQLite3Worker.php', $cancellation);
        $this->context->send($config);
        EventLoop::queue($this->listen(...));
        $this->lastUsedAt = \time();
        $this->deferreds = new \SplQueue;
        $this->onReady = new \SplQueue;
        $this->onClose = new DeferredFuture;
    }

    public function close(): void
    {
        if ($this->onClose->isComplete() || $this->context->isClosed()) {
            return;
        }
        try {
            $this->onClose->complete();
            $this->context->close();
        } catch (StatusError $e) {
            throw new SQLite3ConnectionException($e->getMessage(), $e->getCode(), $e);
        }
    }

    public function isClosed(): bool
    {
        return $this->context->isClosed();
    }

    public function onClose(\Closure $onClose): void
    {
        $this->onClose->getFuture()->finally($onClose);
    }

    private function enqueueDeferred(DeferredFuture $deferred): void
    {
        \assert(!$this->context->isClosed(), "The connection has been closed");
        $this->deferreds->push($deferred);
    }

    private function dequeueDeferred(): DeferredFuture
    {
        \assert(!$this->deferreds->isEmpty(), 'Pending deferred not found when shifting from pending queue');
        return $this->deferreds->shift();
    }

    private function handleResult(DeferredFuture $deferred, mixed $result)
    {
        // SQLite3Exception
        if ($result instanceof SQLite3ChannelException)
            return $this->handleError($deferred, $result);

        // SQLite3Result
        if ($result instanceof SQLite3ChannelResult)
        {
            if ($result->columnCount)
                $result = new SQLite3ConnectionResult($this, $result);
            else
                $result = new SQLite3CommandResult($result);
        }
        // SQLite3Statement
        elseif ($result instanceof SQLite3ChannelStatement) {
            $result = new SQLite3ConnectionStatement($this, $result);
        }
        $deferred->complete($result);
        // new SQLite3Exception("Invalid data received: " . var_export($result, true))
    }

    private function handleError(DeferredFuture $deferred, SQLite3ChannelException $error)
    {
        $query   = $error->query;
        $errcode = $error->code;
        $message = $error->message;
        // SQLite3 exception
        $error = new SQLite3Exception($message, $errcode);
        // SQLite3 query exception error
        if (
            str_ends_with($message, 'incomplete input') ||
            str_ends_with($message, 'syntax error')
        ) {
            $error = new SQLite3QueryError($message, $query);
        }
        // Throw exception
        $deferred->error($error);
    }

    private function listen(): void
    {
        while (!$this->context->isClosed() && $data = $this->context->receive()) {
            $defered = $this->dequeueDeferred();
            EventLoop::queue($this->handleResult(...), $defered, $data);
            $this->ready();
        }
        throw new SQLite3Exception("The connection has been closed");
    }

    /**
     * @param \Closure():void $callback
     */
    private function appendTask(\Closure $callback): void
    {
        if (
            !$this->onReady->isEmpty()
            || !$this->deferreds->isEmpty()
        ) {
            $this->onReady->push($callback);
        } else {
            $callback();
        }
    }

    public function getConfig(): SQLite3Config
    {
        return $this->config;
    }

    public function getLastUsedAt(): int
    {
        return $this->lastUsedAt;
    }

    private function ready(): void
    {
        if (!$this->deferreds->isEmpty()) {
            return;
        }

        if (!$this->onReady->isEmpty()) {
            $this->onReady->shift()();
            return;
        }
    }

    protected function startCommand(\Closure $callback): Future
    {
        if ($this->isClosed()) {
            throw new \Error("The connection has been closed");
        }

        $deferred = new DeferredFuture;
        $this->appendTask(function () use ($callback, $deferred): void {
            $this->enqueueDeferred($deferred);
            $callback();
        });
        return $deferred->getFuture();
    }

    public function query(string $query): Future
    {
        return $this->startCommand(function () use ($query): void {
            $this->context->send(new Query($query));
        });
    }

    public function prepare(string $query): Future
    {
        return $this->startCommand(function () use ($query): void {
            $this->context->send(new Prepare($query));
        });
    }

    public function bindParam(string $stmtId, int|string $param, string $data): void
    {
        $this->startCommand(function () use ($stmtId, $param, $data): void {
            $this->context->send(new StatementOperation($stmtId, 'bindValue', [ $param => $data ]));
            $this->ready();
        });
    }

    public function executeStmt(string $stmtId, array $params): Future
    {
        return $this->startCommand(function () use ($stmtId, $params): void {
            $this->context->send(new StatementOperation($stmtId, 'execute', $params));
        });
    }

    public function resetStmt(string $stmtId): Future
    {
        return $this->startCommand(function () use ($stmtId): void {
            $this->context->send(new StatementOperation($stmtId, 'reset'));
        });
    }

    public function getQueryStmt(string $stmtId): Future
    {
        return $this->startCommand(function () use ($stmtId): void {
            $this->context->send(new StatementOperation($stmtId, 'getSql'));
        });
    }

    public function closeStmt(string $stmtId): void
    {
        $this->startCommand(function () use ($stmtId): void {
            $this->context->send(new StatementOperation($stmtId, 'close'));
            $this->ready();
        });
    }

    public function closeResult(string $resultId): void
    {
        $this->startCommand(function () use ($resultId): void {
            $this->context->send(new ResultOperation($resultId, 'finalize'));
            $this->ready();
        });
    }

    public function getNextResult(string $resultId): Future
    {
        return $this->startCommand(function () use ($resultId): void {
            $this->context->send(new ResultOperation($resultId, 'fetchArray'));
        });
    }

    public function __destruct()
    {
        EventLoop::queue($this->close(...));
    }
}
