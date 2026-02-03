<?php

namespace UniForceMusic\PHPDuckDBCLI\Integrations\Sentience;

use Closure;
use Sentience\Database\Driver;
use Sentience\Database\Databases\DatabaseAbstract;
use Sentience\Database\Queries\Objects\Raw;

class DuckDBDatabase extends DatabaseAbstract
{
    public static function fromFile(
        string $file,
        array $queries = [],
        array $options = [],
        ?Closure $debug = null,
    ): static {
        $adapter = new DuckDBAdapter(
            $file,
            null,
            $queries,
            $options,
            $debug
        );

        $version = $adapter->version();

        $dialect = new DuckDBDialect(
            Driver::PGSQL,
            $version
        );

        return new static($adapter, $dialect);
    }

    public static function memory(
        array $queries = [],
        array $options = [],
        ?Closure $debug = null,
    ): static {
        $adapter = new DuckDBAdapter(
            null,
            null,
            $queries,
            $options,
            $debug
        );

        $version = $adapter->version();

        $dialect = new DuckDBDialect(
            Driver::PGSQL,
            $version
        );

        return new static($adapter, $dialect);
    }

    public function createTable(array|string|Raw $table): CreateTableQuery
    {
        return new CreateTableQuery($this, $this->dialect, $table);
    }
}
