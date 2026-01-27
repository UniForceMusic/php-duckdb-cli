<?php

namespace UniForceMusic\PHPDuckDBCLI;

class Result
{
    public const string REGEX_RESULT_PATTERN = '/^(\┌[\─\┬]+\┐[\S\s]*?\└[\─\┴]+\┘)$\s*/m';

    private bool $parsed = false;
    private array $columns = [];
    private array $rows = [];

    public function __construct(private string $output)
    {
        if (empty($this->output)) {
            $this->parsed = true;

            return;
        }

        $isTableOutput = (bool) preg_match_all(
            static::REGEX_RESULT_PATTERN,
            $output,
            $matches
        );

        if (!$isTableOutput) {
            $this->parsed = true;

            return;
        }

        $this->output = $matches[1][0];
    }

    public function columns(): array
    {
        if (!$this->parsed) {
            $this->parse();
        }

        return $this->columns;
    }

    public function rows(): array
    {
        if (!$this->parsed) {
            $this->parse();
        }

        return $this->rows;
    }

    private function parse(): void
    {
        $lines = explode(PHP_EOL, $this->output);

        $boxTopLine = $lines[0];
        $columnNamesLine = $lines[1];
        $columnTypesLine = $lines[2];
        $rowLines = array_slice(
            $lines,
            4
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
                if (str_contains($line, '─')) {
                    continue;
                }

                if (preg_match('/\S\s*[0-9]+\srows\s*\S/', $line)) {
                    continue;
                }

                $row = [];

                $position = 1;

                foreach ($columnLengths as $columnName => $length) {
                    $row[$columnName] = trim(mb_substr($line, $position, $length));
                    $position += $length + 1;
                }

                $rows[] = $row;
            }
        }

        $this->parsed = true;
        $this->columns = $columns;
        $this->rows = $rows;
    }
}
