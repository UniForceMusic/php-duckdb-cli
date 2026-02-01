<?php

namespace UniForceMusic\PHPDuckDBCLI;

use UniForceMusic\PHPDuckDBCLI\Exceptions\MissingParserException;
use UniForceMusic\PHPDuckDBCLI\Parsers\DuckboxParser;
use UniForceMusic\PHPDuckDBCLI\Parsers\JsonParser;
use UniForceMusic\PHPDuckDBCLI\Parsers\ParserInterface;

enum Mode: string
{
    case DUCKBOX = 'duckbox';
    case JSON = 'json';

    public function getParser(string $output): ParserInterface
    {
        return match ($this) {
            self::DUCKBOX => new DuckboxParser($output),
            self::JSON => new JsonParser($output),
            default => throw new MissingParserException("{$this->value} mode has no parser")
        };
    }
}
