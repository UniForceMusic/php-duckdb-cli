<?php

namespace UniForceMusic\PHPDuckDBCLI\Parsers;

use UniForceMusic\PHPDuckDBCLI\Results\JsonResult;

class JsonParser extends ParserAbstract
{
    public function parse(): JsonResult
    {
        $rows = json_decode($this->output, true) ?? [];

        $columns = array_keys($rows[0] ?? []);

        return new JsonResult($this->output, $columns, $rows);
    }
}
