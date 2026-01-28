<?php

namespace UniForceMusic\PHPDuckDBCLI\Tests;

use UniForceMusic\PHPDuckDBCLI\DuckDB;

abstract class TestCase extends \PHPUnit\Framework\TestCase
{
    protected DuckDB $duckdb;

    protected function setUp(): void
    {
        $this->duckdb = DuckDB::memory();
    }
}
