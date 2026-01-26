<?php

namespace UniForceMusic\PHPDuckDBCLI;

class Error
{
    public function __construct(private string $output)
    {
    }

    public function getError(): string
    {
        return str_replace(PHP_EOL, ' ', $this->output);
    }

    public function getErrorLines(): array
    {
        return explode(PHP_EOL, $this->output);
    }
}
