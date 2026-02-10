<?php

namespace UniForceMusic\PHPDuckDBCLI\Integrations\Sentience;

use Closure;
use Throwable;
use Sentience\Database\Adapters\AdapterAbstract;
use Sentience\Database\Dialects\DialectInterface;
use Sentience\Database\Queries\Objects\QueryWithParams;
use Sentience\Database\Sockets\SocketAbstract;
use UniForceMusic\PHPDuckDBCLI\DuckDB;
use UniForceMusic\PHPDuckDBCLI\Mode;

class DuckDBAdapter extends AdapterAbstract
{
    public const string OPTIONS_DUCKDB_BINARY = 'binary';
    public const string OPTIONS_DUCKDB_READ_ONLY = 'read_only';
    public const string OPTIONS_DUCKDB_MODE = 'mode';
    public const string OPTIONS_DUCKDB_TIMEOUT = 'timeout';

    protected DuckDB $duckdb;

    public function __construct(
        ?string $name,
        protected SocketAbstract|null $socket,
        protected array $queries,
        protected array $options,
        protected ?Closure $debug
    ) {
        $this->duckdb = new DuckDB(
            $name,
            (bool) ($options[static::OPTIONS_DUCKDB_READ_ONLY] ?? false),
            Mode::from($options[static::OPTIONS_DUCKDB_MODE] ?? Mode::JSON->value),
            (string) ($options[static::OPTIONS_DUCKDB_BINARY] ?? DuckDB::BINARY)
        );

        if (array_key_exists(static::OPTIONS_DUCKDB_TIMEOUT, $options)) {
            $this->duckdb->setTimeout((int) $options[static::OPTIONS_DUCKDB_TIMEOUT]);
        }
    }

    public function version(): string
    {
        return substr(
            (new DuckDBResult($this->duckdb->query('SELECT version()')))->scalar(),
            1
        );
    }

    public function dotCommand(string $command): void
    {
        $this->duckdb->dotCommand($command);
    }

    public function exec(string $query): void
    {
        $start = microtime(true);

        try {
            $this->duckdb->exec($query);
        } catch (Throwable $exception) {
            $this->debug($query, $start, $exception);

            throw $exception;
        }

        $this->debug($query, $start);
    }

    public function query(string $query): DuckDBResult
    {
        $start = microtime(true);

        try {
            $result = $this->duckdb->query($query);

            $this->debug($query, $start);

            return new DuckDBResult($result);
        } catch (Throwable $exception) {
            $this->debug($query, $start, $exception);

            throw $exception;
        }
    }

    public function queryWithParams(DialectInterface $dialect, QueryWithParams $queryWithParams, bool $emulatePrepare): DuckDBResult
    {
        $query = $queryWithParams->toSql($dialect);

        return $this->query($query);
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

        return $this->query("SELECT currval('{$name}');", )->scalar();
    }
}
