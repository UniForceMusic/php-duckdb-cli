<?php

namespace UniForceMusic\PHPDuckDBCLI\Integrations\PHP\SQLite3;

use UniForceMusic\PHPDuckDBCLI\DuckDB;
use UniForceMusic\PHPDuckDBCLI\Mode;

class SQLite3 extends \SQLite3
{
    public static $binary = 'duckdb';

    protected DuckDB $duckdb;

    public function __construct(
        string $filename,
        int $flags = SQLITE3_OPEN_READWRITE | SQLITE3_OPEN_CREATE,
        string $encryptionKey = ''
    ) {
        $readOnly = $flags == SQLITE3_OPEN_READONLY;

        $this->duckdb = $filename !== ':memory:'
            ? DuckDB::file($filename, $readOnly, Mode::JSON, static::$binary)
            : DuckDB::memory(Mode::JSON, static::$binary);

        if (!empty($encryptionKey)) {
            throw new \SQLite3Exception('database encryption is not supported');
        }
    }

    public static function open(
        $filename,
        $flags = SQLITE3_OPEN_READWRITE | SQLITE3_OPEN_CREATE,
        $encryptionKey = ''
    ): static {
        return new static($filename, $flags, $encryptionKey);
    }

    public function exec($statement): bool
    {
        $this->duckdb->exec($statement);

        return true;
    }

    public function query($query): SQLite3Result|bool
    {
        return new SQLite3Result($this->duckdb->query($query));
    }

    public function querySingle($query, $entireRow = false): mixed
    {
        $result = $this->duckdb->query($query);

        $row = $result->getRows()[0] ?? [];

        if ($entireRow) {
            return !empty($row) ? $row : null;
        }

        return current($row);
    }

    public function prepare($query): SQLite3Stmt|bool
    {
        return new SQLite3Stmt($this, $query);
    }
}
