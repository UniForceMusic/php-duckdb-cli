<?php

namespace UniForceMusic\PHPDuckDB;

class DuckDB
{
    public const string BINARY = 'duckdb';

    public function __construct(
        protected ?string $file,
        protected string $binary = self::BINARY
    ) {
    }
}
