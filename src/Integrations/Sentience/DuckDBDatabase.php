<?php

namespace UniForceMusic\PHPDuckDBCLI\Integrations\Sentience;

use Closure;
use Sentience\Database\Databases\DatabaseAbstract;
use Sentience\Database\Queries\Objects\Raw;
use Sentience\Database\Queries\SelectQuery;

class DuckDBDatabase extends DatabaseAbstract
{
    public static function connect(
        ?string $file,
        array $queries = [],
        array $options = [],
        ?Closure $debug = null,
    ): static {
        return $file
            ? static::fromFile($file, $queries, $options, $debug)
            : static::memory($queries, $options, $debug);
    }

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

        $dialect = new DuckDBDialect($version);

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

        $dialect = new DuckDBDialect($version);

        return new static($adapter, $dialect);
    }

    public function lastInsertId(string|null $name = null): null|int|string
    {
        $escapedName = substr(
            $this->dialect->escapeString($name),
            1,
            -1
        );

        return $this->adapter->lastInsertId($escapedName);
    }

    public function insert(string|array|Raw $table): InsertQuery
    {
        return new InsertQuery($this, $this->dialect, $table);
    }

    public function createTable(array|string|Raw $table): CreateTableQuery
    {
        return new CreateTableQuery($this, $this->dialect, $table);
    }
}
