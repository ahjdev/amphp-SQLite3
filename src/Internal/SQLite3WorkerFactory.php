<?php declare(strict_types=1);

namespace Amp\SQLite3\Internal;

use Amp\Cancellation;
use Amp\ForbidCloning;
use Amp\ForbidSerialization;
use Amp\SQLite3\SQLite3Config;
use Amp\SQLite3\Internal;
use Amp\Parallel\Worker\Worker;
use Amp\Parallel\Worker\WorkerFactory;
use function Amp\Parallel\Worker\workerFactory;

final class SQLite3WorkerFactory implements WorkerFactory
{
    use ForbidCloning;
    use ForbidSerialization;

    public function __construct(
        private readonly ?SQLite3Config $config = null,
    ) {
    }

    public function create(?Cancellation $cancellation = null): Worker
    {
        $factory = workerFactory();
        $worker = $factory->create($cancellation);
        $worker->submit(new Internal\SQLite3Task('init', [$this->config]));
        return $worker;
    }
}
