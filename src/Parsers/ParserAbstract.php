<?php

namespace UniForceMusic\PHPDuckDBCLI\Parsers;

abstract class ParserAbstract implements ParserInterface
{
    public function __construct(protected string $output)
    {
    }
}
