<?php

namespace UniForceMusic\PHPDuckDBCLI\Results;

abstract class ResultAbstract implements ResultInterface
{
    public function __construct(
        protected string $output,
        protected array $columns,
        protected array $rows
    ) {
    }

    public function getRawOutput(): string
    {
        return $this->output;
    }

    public function getColumns(): array
    {
        return $this->columns;
    }

    public function getRows(): array
    {
        return $this->rows;
    }
}
