<?php

namespace UniForceMusic\PHPDuckDBCLI\Integrations\Sentience;

use Sentience\Database\Dialects\PgSQLDialect;
use Sentience\Database\Driver;
use Sentience\Database\Queries\Enums\ConditionEnum;
use Sentience\Database\Queries\Enums\TypeEnum;
use Sentience\Database\Queries\Objects\Condition;
use Sentience\Database\Queries\Objects\QueryWithParams;

class DuckDBDialect extends PgSQLDialect
{
    public function __construct(protected int|string $duckDbVersion)
    {
        parent::__construct(Driver::PGSQL, '10.0');
    }

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

    protected function buildConditionRegex(string &$query, array &$params, Condition $condition): void
    {
        if ($condition->condition == ConditionEnum::NOT_REGEX) {
            $query .= 'NOT ';
        }

        $query .= sprintf(
            'REGEXP_MATCHES(%s, %s, %s)',
            $this->escapeIdentifier($condition->identifier),
            $this->buildQuestionMarks($params, $condition->value[0]),
            $this->buildQuestionMarks($params, $condition->value[1])
        );
    }

    public function type(TypeEnum $type, ?int $size = null): string
    {
        return match ($type) {
            TypeEnum::INT => $size > 32 ? 'INT64' : 'INT32',
            default => parent::type($type, $size)
        };
    }
}
