<?php

namespace UniForceMusic\PHPDuckDB;

use UniForceMusic\PHPDuckDB\Exceptions\DuckDBException;

class DuckDB
{
    public const string BINARY = 'duckdb';

    protected Connection $connection;

    public function __construct(
        protected ?string $file,
        protected string $binary = self::BINARY
    ) {
        $this->connection = new Connection($binary, $file);
    }

    public function exec(string $statement): void
    {
        $this->connection->execute($statement);
    }

    public function query(string $query): Result
    {
        $result = $this->connection->execute($query);

        if ($result instanceof Error) {
            throw new DuckDBException($result->getError());
        }

        return $result;
    }

    public function prepared(string $query, array $params = []): Result
    {
        $result = $this->connection->execute($query);

        if ($result instanceof Error) {
            throw new DuckDBException($result->getError());
        }

        return $result;
    }
}
