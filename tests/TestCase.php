<?php

namespace UniForceMusic\PHPDuckDBCLI\Tests;

use UniForceMusic\PHPDuckDBCLI\DuckDB;
use UniForceMusic\PHPDuckDBCLI\Mode;

abstract class TestCase extends \PHPUnit\Framework\TestCase
{
    protected DuckDB $duckdb;

    protected function setUp(): void
    {
        $this->duckdb = DuckDB::memory();
    }
}
