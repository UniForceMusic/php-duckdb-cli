<?php

namespace UniForceMusic\PHPDuckDBCLI\Results;

class EmptyResult extends ResultAbstract
{
    protected string $output = '';
    protected array $columns = [];
    protected array $rows = [];

    public function __construct()
    {
    }
}
