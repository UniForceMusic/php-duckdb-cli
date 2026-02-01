<?php

namespace UniForceMusic\PHPDuckDBCLI\Parsers;

use UniForceMusic\PHPDuckDBCLI\Results\JsonResult;

class JsonParser extends ParserAbstract
{
    public const string REGEX_JSON_OUTPUT_PATTERN = '/^(\[[\S\s]*\])$\s*/m';
    public const string OUTPUT_FALLBACK = '[]';

    public function parse(): JsonResult
    {
        preg_match(self::REGEX_JSON_OUTPUT_PATTERN, $this->output, $match);

        $output = $match[0] ?? self::OUTPUT_FALLBACK;

        $rows = json_decode($output, true) ?? [];

        $columns = array_keys($rows[0] ?? []);

        return new JsonResult($output, $columns, $rows);
    }
}
