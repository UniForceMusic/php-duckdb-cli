<?php

namespace UniForceMusic\PHPDuckDBCLI\Parsers;

use UniForceMusic\PHPDuckDBCLI\Results\ResultInterface;

interface ParserInterface
{
    public function parse(): ResultInterface;
}
