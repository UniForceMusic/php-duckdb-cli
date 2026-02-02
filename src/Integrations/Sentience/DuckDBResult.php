<?php

namespace UniForceMusic\PHPDuckDBCLI\Integrations\Sentience;

use Sentience\Database\Results\ResultAbstract;
use UniForceMusic\PHPDuckDBCLI\Results\ResultInterface;

class DuckDBResult extends ResultAbstract
{
    private array $rows = [];

    public function __construct(private ResultInterface $result)
    {
        $this->rows = $result->getRows();
    }

    public function columns(): array
    {
        return $this->result->getColumns();
    }

    public function fetchAssoc(): ?array
    {
        return $this->rows[0] ?? null;
    }

    public function fetchAssocs(): array
    {
        return $this->rows;
    }
}
