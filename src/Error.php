<?php

namespace UniForceMusic\PHPDuckDB;

class Error
{
    public function __construct(protected string $output)
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
