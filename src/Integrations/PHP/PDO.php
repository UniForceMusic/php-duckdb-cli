<?php

namespace UniForceMusic\PHPDuckDBCLI\Integrations\PHP;

use PDOStatement;
use UniForceMusic\PHPDuckDBCLI\DuckDB;
use UniForceMusic\PHPDuckDBCLI\Mode;

// Array
// (
//     [1] => beginTransaction
//     [2] => commit
//     [3] => errorCode
//     [4] => errorInfo
//     [5] => exec
//     [6] => getAttribute
//     [7] => getAvailableDrivers
//     [8] => inTransaction
//     [9] => lastInsertId
//     [10] => prepare
//     [11] => query
//     [12] => quote
//     [13] => rollBack
//     [14] => setAttribute
// )

class PDO extends \PDO
{
    public const int DUCKDB_ATTR_BINARY = 0;
    public const int DUCKDB_ATTR_READ_ONLY = 1;
    public const int DUCKDB_ATTR_MODE = 2;
    public const int DUCKDB_ATTR_TIMEOUT = 3;

    protected DuckDB $duckdb;

    public function __construct(
        string $dsn,
        ?string $username = null,
        ?string $password = null,
        array $options = []
    ) {
        [$driver, $file] = explode(':', $dsn, 2);

        $this->duckdb = new DuckDB(
            $file != ':memory:' ? $file : null,
            (bool) ($options[static::DUCKDB_ATTR_READ_ONLY] ?? false),
            Mode::from($options[static::DUCKDB_ATTR_MODE] ?? Mode::JSON->value),
            (string) ($options[static::DUCKDB_ATTR_BINARY] ?? DuckDB::BINARY)
        );

        if (array_key_exists(static::DUCKDB_ATTR_TIMEOUT, $options)) {
            $this->duckdb->setTimeout((int) $options[static::DUCKDB_ATTR_TIMEOUT]);
        }
    }

    public function exec(string $statement): int
    {
        return (int) $this->duckdb->exec($statement);
    }

    // public function query($query): PDOStatement
    // {
    //     $this->duckdb->exec($statement);

    //     return -1;
    // }
}
