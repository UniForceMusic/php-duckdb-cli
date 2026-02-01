<?php

namespace UniForceMusic\PHPDuckDBCLI\Results;

interface ResultInterface
{
    public function getRawOutput(): string;
    public function getColumns(): array;
    public function getRows(): array;
}
