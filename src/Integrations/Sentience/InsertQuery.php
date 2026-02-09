<?php

namespace UniForceMusic\PHPDuckDBCLI\Integrations\Sentience;

use Sentience\Database\Queries\Objects\QueryWithParams;

/**
 * @property DuckDBDatabase $database
 * @property DuckDBDialect $dialect
 */
class InsertQuery extends \Sentience\Database\Queries\InsertQuery
{
    public function toQueryWithParams(): QueryWithParams
    {
        if ($this->lastInsertId && (is_null($this->returning) || count($this->returning) > 0)) {
            $this->returning[] = $this->lastInsertId;
        }

        return parent::toQueryWithParams();
    }
}
