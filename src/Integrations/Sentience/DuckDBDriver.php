<?php

namespace UniForceMusic\PHPDuckDBCLI\Integrations\Sentience;

use Closure;
use Sentience\Database\DriverInterface;
use Sentience\Database\Sockets\SocketAbstract;

class DuckDBDriver implements DriverInterface
{
    public function name(): string
    {
        return 'duckdb';
    }

    public function getAdapter(
        string $name,
        ?SocketAbstract $socket,
        array $queries,
        array $options,
        ?Closure $debug,
        bool $usePDOAdapter = false
    ): DuckDBAdapter {
        return new DuckDBAdapter(
            $name,
            $socket,
            $queries,
            $options,
            $debug
        );
    }

    public function getDialect(int|string $version): DuckDBDialect
    {
        return new DuckDBDialect($version);
    }
}
