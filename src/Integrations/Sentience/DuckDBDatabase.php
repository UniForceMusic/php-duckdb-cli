<?php

namespace UniForceMusic\PHPDuckDBCLI\Integrations\Sentience;

use Closure;
use Sentience\Database\Databases\DatabaseAbstract;
use Sentience\Database\Queries\Interfaces\Sql;
use Sentience\Database\Queries\Objects\Raw;

class DuckDBDatabase extends DatabaseAbstract
{
    public static function connect(
        ?string $file,
        array $queries = [],
        array $options = [],
        ?Closure $debug = null,
    ): static {
        return $file
            ? static::file($file, $queries, $options, $debug)
            : static::memory($queries, $options, $debug);
    }

    public static function file(
        string $file,
        array $queries = [],
        array $options = [],
        ?Closure $debug = null,
    ): static {
        $driver = new DuckDBDriver();

        $adapter = new DuckDBAdapter(
            $file,
            null,
            $queries,
            $options,
            $debug
        );

        $version = $adapter->version();

        $dialect = new DuckDBDialect($driver, $version, $options);

        return new static($adapter, $dialect);
    }

    public static function memory(
        array $queries = [],
        array $options = [],
        ?Closure $debug = null,
    ): static {
        $driver = new DuckDBDriver();

        $adapter = new DuckDBAdapter(
            null,
            null,
            $queries,
            $options,
            $debug
        );

        $version = $adapter->version();

        $dialect = new DuckDBDialect($driver, $version, $options);

        return new static($adapter, $dialect);
    }

    public function dotCommand(string $command): void
    {
        /** @var DuckDBAdapter $this->adapter */

        $this->adapter->dotCommand($command);
    }

    public function lastInsertId(null|string $name = null): null|int|string
    {
        if (!$name) {
            return null;
        }

        $escapedName = substr(
            $this->dialect->escapeString($name),
            1,
            -1
        );

        return $this->adapter->lastInsertId($escapedName);
    }

    public function insert(string|array|Sql $table): InsertQuery
    {
        return new InsertQuery($this, $this->dialect, $table);
    }

    public function createTable(array|string|Sql $table): CreateTableQuery
    {
        return new CreateTableQuery($this, $this->dialect, $table);
    }
}
