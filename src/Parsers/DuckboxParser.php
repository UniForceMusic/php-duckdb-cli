<?php

namespace UniForceMusic\PHPDuckDBCLI\Parsers;

use UniForceMusic\PHPDuckDBCLI\Results\DuckboxResult;

class DuckboxParser extends ParserAbstract
{
    public const string REGEX_DUCKBOX_OUTPUT_PATTERN = '/^(\┌[\─\┬]+\┐[\S\s]*?\└[\─\┴]+\┘)$\s*/m';
    public const string OUTPUT_FALLBACK = '┌──┐' . PHP_EOL . '│  │' . PHP_EOL . '└──┘';

    public function parse(): DuckboxResult
    {
        preg_match(self::REGEX_DUCKBOX_OUTPUT_PATTERN, $this->output, $match);

        $output = $match[0] ?? self::OUTPUT_FALLBACK;

        $lines = explode(PHP_EOL, $output);

        $boxTopLine = $lines[0];
        $columnNamesLine = $lines[1];
        $columnTypesLine = $lines[2];
        $rowLines = array_slice(
            $lines,
            4,
            -1
        );

        $boxTops = explode(
            '┬',
            mb_substr(
                $boxTopLine,
                1,
                -1
            )
        );

        $columnLengths = [];
        $columns = [];
        $rows = [];

        $position = 1;

        foreach ($boxTops as $boxTop) {
            $length = mb_strlen($boxTop);

            $columnName = trim(mb_substr($columnNamesLine, $position, $length));
            $columnType = trim(mb_substr($columnTypesLine, $position, $length));

            $columnLengths[$columnName] = $length;
            $columns[$columnName] = $columnType;

            $position += $length + 1;
        }

        if (!preg_match('/\│\s*0\srows\s*\│/', implode('', $rowLines))) {
            foreach ($rowLines as $line) {
                if (preg_match('/\S\s*[0-9]+\srows\s*\S/', $line)) {
                    continue;
                }

                $row = [];

                $position = 1;

                foreach ($columnLengths as $columnName => $length) {
                    $type = strtolower($columns[$columnName]);
                    $value = rtrim(mb_substr($line, $position + 1, $length - 1));

                    $row[$columnName] = $this->castValue($type, $value);

                    $position += $length + 1;
                }

                $rows[] = $row;
            }
        }

        return new DuckboxResult($output, $columns, $rows);
    }

    private function castValue(string $type, string $value): mixed
    {
        if (strtolower($value) == 'null') {
            return null;
        }

        if (str_contains($type, 'int')) {
            return (int) $value;
        }

        foreach (['real', 'float', 'numeric', 'number'] as $wildcard) {
            if (str_contains($type, $wildcard)) {
                return (float) $value;
            }
        }

        return $value;
    }
}
