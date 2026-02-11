<?php

namespace UniForceMusic\PHPDuckDBCLI\Integrations\PHP\SQLite3;

use UniForceMusic\PHPDuckDBCLI\Results\ResultInterface;

class SQLite3Result extends \SQLite3Result
{
    protected array $columns;
    protected array $rows;
    protected int $rowIndex = 0;

    public function __construct(protected ResultInterface $result)
    {
        $this->columns = $result->getColumns();
        $this->rows = $result->getRows();
    }

    public function numColumns(): int
    {
        return count($this->result->getColumns());
    }

    public function columnName($column): string
    {
        return $this->result->getColumns()[$column] ?? throw new \SQLite3Exception('column at this index does not exist');
    }

    public function columnType($column): int
    {
        return SQLITE3_NULL;
    }

    public function fetchArray($mode = SQLITE3_ASSOC): array|bool
    {
        if ($this->rowIndex > count($this->columns) - 1) {
            return false;
        }

        $array = match ($mode) {
            SQLITE3_ASSOC => $this->rows[$this->rowIndex],
            SQLITE3_NUM => array_values($this->rows[$this->rowIndex]),
            SQLITE3_BOTH => [...array_values($this->rows[$this->rowIndex]), ...$this->rows[$this->rowIndex]]
        };

        $this->rowIndex++;

        return $array;
    }

    public function reset(): void
    {
        $this->rowIndex = 0;
    }

    public function finalize(): void
    {
        $this->columns = [];
        $this->rows = [];
        $this->rowIndex = 0;
    }
}
