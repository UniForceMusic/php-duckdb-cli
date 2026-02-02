<?php

namespace UniForceMusic\PHPDuckDBCLI\Integrations\Sentience;

use Closure;
use Sentience\Database\Driver;
use Sentience\Database\Databases\DatabaseAbstract;

class DuckDBDatabase extends DatabaseAbstract
{
    public static function fromFile(
        string $file,
        array $queries = [],
        array $options = [],
        ?Closure $debug = null,
    ): static {
        $adapter = new DuckDBAdapter(
            Driver::SQLITE,
            $file,
            null,
            $queries,
            $options,
            $debug
        );

        $version = $adapter->version();

        $dialect = new DuckDBDialect(
            Driver::SQLITE,
            $version
        );

        return new static($adapter, $dialect);
    }

    public static function fromMemory(
        array $queries = [],
        array $options = [],
        ?Closure $debug = null,
    ): static {
        $adapter = new DuckDBAdapter(
            Driver::SQLITE,
            null,
            null,
            $queries,
            $options,
            $debug
        );

        $version = $adapter->version();

        $dialect = new DuckDBDialect(
            Driver::SQLITE,
            $version
        );

        return new static($adapter, $dialect);
    }
}
