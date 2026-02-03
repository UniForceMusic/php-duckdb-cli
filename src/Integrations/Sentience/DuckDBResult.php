<?php

namespace UniForceMusic\PHPDuckDBCLI\Integrations\Sentience;

use Sentience\Database\Results\Result;
use UniForceMusic\PHPDuckDBCLI\Results\ResultInterface;

class DuckDBResult extends Result
{
    protected array $columns;
    protected array $rows;

    public function __construct(private ResultInterface $result)
    {
        $this->columns = $result->getColumns();
        $this->rows = $result->getRows();
    }
}
