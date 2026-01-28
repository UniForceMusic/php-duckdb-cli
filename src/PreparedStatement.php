<?php

namespace UniForceMusic\PHPDuckDBCLI;

use DateTimeInterface;
use Throwable;
use UniForceMusic\PHPDuckDBCLI\Exceptions\PreparedStatementException;

class PreparedStatement
{
    public const array ESCAPE_CHARS = [
        '\\' => '\\\\',
        "\n" => '\\n',
        "\r" => '\\r',
        "\t" => '\\t',
        "\0" => '',
        "\b" => '\\b',
        "\x1A" => '\\x1A',
        "\f" => '\\f',
        "\v" => '\\v'
    ];
    public const string INI_PCRE_JIT = 'pcre.jit';
    public const string REGEX_PATTERN_QUESTION_MARKS = '/(?:\'(?:\\\\.|[^\\\\\'])*\'|\"(?:\\\\.|[^\\\\\"])*\"|(\?)(?=(?:[^\'\"\\\\]|\'(?:\\\\.|[^\\\\\'])*\'|\"(?:\\\\.|[^\\\\\"])*\"|)*$)|(?:\-\-[^\r\n]*|\/\*[\s\S]*?\*\/|\#.*))/m';
    public const string REGEX_PATTERN_NAMED_PARAMS = '/(?:\'(?:\\\\.|[^\\\\\'])*\'|\"(?:\\\\.|[^\\\\\"])*\"|(\:\w+)(?=(?:[^\'\"\\\\]|\'(?:\\\\.|[^\\\\\'])*\'|\"(?:\\\\.|[^\\\\\"])*\"|)*$)|(?:\-\-[^\r\n]*|\/\*[\s\S]*?\*\/|\#.*))/m';

    public function __construct(
        public string $query,
        public array $params = []
    ) {
    }

    public function toSql(): string
    {
        if (count($this->params) == 0) {
            return $this->query;
        }

        $this->namedParamsToQuestionMarks();

        $params = array_map(
            fn(mixed $param): mixed => $this->compileValueToSQL($param),
            $this->params
        );

        foreach ($params as $key => $value) {
            if (!ctype_digit((string) $key)) {
                return $this->toSqlNamedParams($params);
            }
        }

        return $this->toSqlQuestionMarks($params);
    }

    private function namedParamsToQuestionMarks(): void
    {
        if (count($this->params) == 0) {
            return;
        }

        $params = [];

        $query = $this->pregReplaceCallback(
            static::REGEX_PATTERN_NAMED_PARAMS,
            function (array $match) use (&$params): string {
                if (!$this->isQuestionMarkOrNamedParamMatch($match)) {
                    return $match[0];
                }

                $key = $match[1];

                if (!array_key_exists($key, $this->params)) {
                    $this->throwNamedParamDoesNotExistException($key);
                }

                $params[] = $this->params[$key];

                return '?';
            },
            $this->query
        );

        if (count($params) == 0) {
            return;
        }

        $this->query = $query;
        $this->params = $params;
    }

    private function toSqlQuestionMarks(array $params): string
    {
        $index = 0;

        return $this->pregReplaceCallback(
            static::REGEX_PATTERN_QUESTION_MARKS,
            function (array $match) use ($params, &$index): string {
                if (!$this->isQuestionMarkOrNamedParamMatch($match)) {
                    return $match[0];
                }

                if (!array_key_exists($index, $params)) {
                    throw new PreparedStatementException('question mark and value count do not match');
                }

                $value = $params[$index];

                $index++;

                return (string) $value;
            },
            $this->query
        );
    }

    private function toSqlNamedParams(array $params): string
    {
        return $this->pregReplaceCallback(
            static::REGEX_PATTERN_NAMED_PARAMS,
            function (array $match) use ($params): string {
                if (!$this->isQuestionMarkOrNamedParamMatch($match)) {
                    return $match[0];
                }

                $key = $match[1];

                if (!array_key_exists($key, $params)) {
                    $this->throwNamedParamDoesNotExistException($key);
                }

                return (string) $params[$key];
            },
            $this->query
        );
    }

    private function pregReplaceCallback(string|array $pattern, callable $callback, string|array $subject): null|string|array
    {
        $ini = ini_get(static::INI_PCRE_JIT);

        ini_set(static::INI_PCRE_JIT, '0');

        try {
            return preg_replace_callback($pattern, $callback, $subject);
        } catch (Throwable $exception) {
            throw $exception;
        } finally {
            if (!is_bool($ini)) {
                ini_set(static::INI_PCRE_JIT, $ini);
            }
        }
    }

    private function isQuestionMarkOrNamedParamMatch(array $match): bool
    {
        return count($match) > 1;
    }

    private function throwNamedParamDoesNotExistException(string $key): void
    {
        throw new PreparedStatementException("named param {$key} does not exist");
    }

    private function compileValueToSQL(null|bool|int|float|string|DateTimeInterface $value): string
    {
        if (is_null($value)) {
            return 'NULL';
        }

        if (is_bool($value)) {
            return var_export($value);
        }

        if (is_string($value)) {
            return "'" . strtr($value, [...static::ESCAPE_CHARS, "'" => "''"]) . "'";
        }

        if ($value instanceof DateTimeInterface) {
            return $this->compileValueToSQL($value->format('Y-m-d H:i:s.uP'));
        }

        return (string) $value;
    }
}
