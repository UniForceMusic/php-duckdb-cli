<?php

namespace UniForceMusic\PHPDuckDBCLI\Integrations\Sentience;

use Sentience\Database\Queries\Enums\TypeEnum;
use Sentience\Database\Queries\Objects\Column;
use Sentience\Database\Queries\Objects\QueryWithParams;
use Sentience\Database\Queries\Objects\Raw;
use Sentience\Database\Queries\Query;
use Sentience\Database\Results\ResultInterface;

/**
 * @property DuckDBDatabase $database
 * @property DuckDBDialect $dialect
 */
class CreateTableQuery extends \Sentience\Database\Queries\CreateTableQuery
{
    protected bool $createOrReplaceSequences = false;

    public function execute(bool $emulatePrepare = false): ResultInterface
    {
        return $this->database->transaction(
            function () use ($emulatePrepare): ResultInterface {
                $sequences = $this->generateSequences();

                $sequenceQueries = $this->generateSequenceQueuries($sequences);

                foreach ($sequenceQueries as $sequenceQuery) {
                    $this->database->exec($sequenceQuery->toSql($this->dialect));
                }

                $bigIntType = $this->dialect->type(TypeEnum::INT, 64);

                foreach ($this->columns as $column) {
                    if (!array_key_exists($column->name, $sequences)) {
                        continue;
                    }

                    $sequence = $sequences[$column->name];

                    $column->type = $bigIntType;
                    $column->default = Query::raw(
                        sprintf(
                            'nextval(%s)',
                            $this->dialect->escapeString($sequence)
                        )
                    );
                    $column->generatedByDefaultAsIdentity = false;
                }

                return parent::execute($emulatePrepare);
            }
        );
    }

    public function toSql(): string
    {
        $query = parent::toSql();

        $sequences = $this->generateSequences();

        $sequenceQueries = $this->generateSequenceQueuries($sequences);

        return implode(
            '; ',
            [
                ...array_map(
                    fn(QueryWithParams $queryWithParams): string => $queryWithParams->toSql($this->dialect),
                    $sequenceQueries
                ),
                $query
            ]
        );
    }

    protected function generateSequences(): array
    {
        $sequences = [];

        $table = $this->getTable($this->table);

        foreach ($this->columns as $column) {
            if (!$this->isSerialColumn($column)) {
                continue;
            }

            $sequence = sprintf(
                '%s_%s_sequence',
                $table,
                $column->name
            );

            $sequences[$column->name] = $sequence;
        }

        return $sequences;
    }

    protected function generateSequenceQueuries(array $sequences): array
    {
        $queries = [];

        foreach ($sequences as $sequence) {
            $queries[] = $this->dialect->createSequence(
                $this->ifNotExists,
                $this->createOrReplaceSequences,
                $sequence
            );
        }

        return $queries;
    }

    protected function isSerialColumn(Column $column): bool
    {
        return $column->generatedByDefaultAsIdentity || str_contains(strtoupper($column->type), 'SERIAL');
    }

    protected function getTable(string|array|Raw $table): string
    {
        if ($table instanceof Raw) {
            return $table->sql;
        }

        if (is_array($table)) {
            return $this->getTable(end($table));
        }

        return $this->table;
    }

    public function createOrReplaceSequences(): static
    {
        $this->createOrReplaceSequences = true;

        return $this;
    }
}
