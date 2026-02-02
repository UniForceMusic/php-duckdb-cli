<?php

namespace UniForceMusic\PHPDuckDBCLI\Integrations\Sentience;

use Closure;
use Sentience\Database\Adapters\AdapterAbstract;
use Sentience\Database\Dialects\DialectInterface;
use Sentience\Database\Driver;
use Sentience\Database\Queries\Objects\QueryWithParams;
use Sentience\Database\Sockets\SocketAbstract;
use UniForceMusic\PHPDuckDBCLI\DuckDB;

class DuckDBAdapter extends AdapterAbstract
{
    protected DuckDB $duckdb;

    public function __construct(
        Driver $driver,
        ?string $name,
        SocketAbstract|null $socket,
        array $queries,
        array $options,
        ?Closure $debug
    ) {
        parent::__construct(
            $driver,
            $name ?? '',
            $socket,
            $queries,
            $options,
            $debug
        );

        $this->duckdb = new DuckDB($name);
    }

    public function version(): string
    {
        return substr(
            $this->query('SELECT version()')->scalar(),
            1
        );
    }

    public function exec(string $query): void
    {
        $this->duckdb->exec($query);
    }

    public function query(string $query): DuckDBResult
    {
        return new DuckDBResult($this->duckdb->query($query));
    }

    public function queryWithParams(DialectInterface $dialect, QueryWithParams $queryWithParams, bool $emulatePrepare): DuckDBResult
    {
        $query = $queryWithParams->namedParamsToQuestionMarks()->toSql($dialect);

        return new DuckDBResult($this->duckdb->query($query));
    }

    public function beginTransaction(DialectInterface $dialect, ?string $name = null): void
    {
        $this->duckdb->beginTransation();

    }

    public function commitTransaction(DialectInterface $dialect, ?string $name = null): void
    {
        $this->duckdb->commitTransation();

    }

    public function rollbackTransaction(DialectInterface $dialect, ?string $name = null): void
    {
        $this->duckdb->rollbackTransation();
    }

    public function inTransaction(): bool
    {
        return $this->duckdb->inTransaction();
    }

    public function lastInsertId(?string $name = null): null|int|string
    {
        if (!$name) {
            return null;
        }

        return $this->query(
            sprintf(
                "SELECT currval('%s');",
                $name
            )
        )->scalar();
    }
}
