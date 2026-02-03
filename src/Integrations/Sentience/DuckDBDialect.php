<?php

namespace UniForceMusic\PHPDuckDBCLI\Integrations\Sentience;

use Sentience\Database\Dialects\PgSQLDialect;
use Sentience\Database\Queries\Enums\TypeEnum;
use Sentience\Database\Queries\Objects\QueryWithParams;

class DuckDBDialect extends PgSQLDialect
{
    public function createSequence(
        bool $ifNotExists,
        string $name
    ): QueryWithParams {
        $query = 'CREATE SEQUENCE';

        if ($ifNotExists) {
            $query .= ' IF NOT EXISTS';
        }

        $query .= ' ';
        $query .= $this->escapeIdentifier($name);

        return new QueryWithParams($query);
    }

    public function dropSequence(
        bool $ifExists,
        string $name
    ): QueryWithParams {
        $query = 'DROP SEQUENCE';

        if ($ifExists) {
            $query .= ' IF EXISTS';
        }

        $query .= ' ';
        $query .= $this->escapeIdentifier($name);

        return new QueryWithParams($query);
    }

    public function type(TypeEnum $type, ?int $size = null): string
    {
        return match ($type) {
            TypeEnum::INT => $size > 32 ? 'INT64' : 'INT32',
            default => parent::type($type, $size)
        };
    }
}
