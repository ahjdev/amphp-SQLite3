<?php declare(strict_types=1);

namespace Amp\SQLite3\Internal;

use Amp\Cancellation;
use Amp\DeferredFuture;
use Amp\Future;
use Amp\Parallel\Context\Context;
use Amp\Parallel\Context\StatusError;
use Amp\Sql\SqlTransientResource;
use Amp\SQLite3\Internal\SQLite3Worker\SQLite3Command\Prepare;
use Amp\SQLite3\Internal\SQLite3Worker\SQLite3Command\Query;
use Amp\SQLite3\Internal\SQLite3Worker\SQLite3Command\StatementOperation;
use Amp\SQLite3\Internal\SQLite3Worker\SQLite3WorkerCommandResult;
use Amp\SQLite3\Internal\SQLite3Worker\SQLite3WorkerResult;
use Amp\SQLite3\Internal\SQLite3Worker\SQLite3WorkerStatement;
use Amp\SQLite3\SQLite3Config;
use Amp\SQLite3\SQLite3ConnectionException;
use Amp\SQLite3\SQLite3Exception;
use Revolt\EventLoop;
use Throwable;
use function Amp\Parallel\Context\contextFactory;

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
        $this->deferreds  = new \SplQueue();
        $this->onReady    = new \SplQueue();
        $this->onClose    = new DeferredFuture();
    }

    public function close(): void
    {
        if ($this->onClose->isComplete()) {
            return;
        }
        try {
            if (!$this->context->isClosed()) {
                return;
            }
            $this->context->close();
        } catch (StatusError $e) {
            throw new SQLite3ConnectionException($e->getMessage(), $e->getCode(), $e);
        }
        $this->onClose->complete();
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

    private function listen(): void
    {
        while($data = $this->context->receive()) {
            if ($data instanceof Throwable) {
                $this->dequeueDeferred()->error($data);
                $this->ready();
                continue;
            }
            $data = match (true) {
                \is_bool($data), \is_string($data) => $data,
                $data instanceof SQLite3WorkerResult        => new SQLite3ConnectionResult($data),
                $data instanceof SQLite3WorkerCommandResult => new SQLite3CommandResult($data),
                $data instanceof SQLite3WorkerStatement     => new SQLite3ConnectionStatement($this, $data),
                default => new SQLite3Exception("Invalid data received: " . $data),
            };
            $this->dequeueDeferred()->complete($data);
            $this->ready();
        }
        throw new SQLite3Exception("The connection has been closed");
    }

    /**
     * @param \Closure():void $callback
     */
    private function appendTask(\Closure $callback): void
    {
        if (!$this->onReady->isEmpty()
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

    public function bindParam(int $stmtId, int|string $param, string $data): void
    {
        $this->appendTask(function () use ($stmtId, $param, $data): void {
            $this->context->send(new StatementOperation($stmtId, 'bindValue', [$param => $data]));
            $this->ready();
        });
    }

    public function executeStmt(int $stmtId, array $params): Future
    {
        return $this->startCommand(function () use ($stmtId, $params): void {
            $this->context->send(new StatementOperation($stmtId, 'execute', $params));
        });
    }

    public function resetStmt(int $stmtId): Future
    {
        return $this->startCommand(function () use ($stmtId): void {
            $this->context->send(new StatementOperation($stmtId, 'reset'));
        });
    }

    public function getQueryStmt(int $stmtId): Future
    {
        return $this->startCommand(function () use ($stmtId): void {
            $this->context->send(new StatementOperation($stmtId, 'getSql'));
        });
    }

    public function closeStmt(int $stmtId): void
    {
        $this->appendTask(function () use ($stmtId): void {
            $this->context->send(new StatementOperation($stmtId, 'close'));
            $this->ready();
        });
    }

    public function __destruct()
    {
        $this->close();
    }
}
