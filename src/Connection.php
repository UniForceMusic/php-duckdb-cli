<?php

namespace UniForceMusic\PHPDuckDBCLI;

use UniForceMusic\PHPDuckDBCLI\Exceptions\ConnectionException;
use UniForceMusic\PHPDuckDBCLI\Exceptions\DuckDBException;

class Connection
{
    public const string REGEX_RESULT_PATTERN = '/^(\┌[\─\┬]+\┐[\S\s]*?\└[\─\┴]+\┘)$\s*/m';
    public const string REGEX_ERROR_PATTERN = '/^([A-Za-z\-\_\s]+\sError\:?[\S\s]+)/m';
    public const string REGEX_TAIL_PATTERN = '/^(\┌[\─\┬]+\┐?[\S\s]*%s?[\S\s]*?\└[\─\┴]+\┘)$\s*/m';
    public const int PIPE_STDIN = 0;
    public const int PIPE_STDOUT = 1;
    public const int PIPE_STDERR = 2;
    public const int USLEEP_TIME = 100;

    private mixed $process;
    private array $pipes = [];

    public function __construct(string $binary, ?string $file)
    {
        $command = sprintf(
            '%s -noheader',
            escapeshellarg($binary)
        );

        if ($file) {
            $command .= ' ';
            $command .= escapeshellarg($file);
        }

        $this->process = proc_open(
            $command,
            [
                ['pipe', 'r'],
                ['pipe', 'w'],
                ['pipe', 'w']
            ],
            $this->pipes
        );

        if (!is_resource($this->process)) {
            throw new ConnectionException('unable to open process');
        }

        foreach ($this->pipes as $pipe) {
            stream_set_blocking($pipe, false);
        }
    }

    public function execute(string $sql): Result|Error
    {
        if (!preg_match('/;\s*$/', $sql)) {
            $sql .= ';';
        }

        [$tailStatement, $tailHash] = $this->generateTailStatement();

        fwrite($this->pipes[self::PIPE_STDIN], $sql);
        fwrite($this->pipes[self::PIPE_STDIN], $tailStatement);
        fwrite($this->pipes[self::PIPE_STDIN], PHP_EOL);

        $output = '';

        while (true) {
            $output .= stream_get_contents($this->pipes[self::PIPE_STDOUT]);
            $output .= stream_get_contents($this->pipes[self::PIPE_STDERR]);

            if (!proc_get_status($this->process)['running']) {
                throw new DuckDBException('DuckDB process has quit unexpectedly');
            }

            $output = $this->isFinishedGeneratingOutput($output, $tailHash);

            if (!is_null($output)) {
                break;
            }

            usleep(static::USLEEP_TIME);
        }

        $output ??= '';

        return $this->isErrorOutput($output)
            ? new Error($output)
            : new Result($output);
    }

    private function generateTailStatement(): array
    {
        $hash = md5(microtime());

        return [
            "SELECT '{$hash}' AS _;",
            $hash
        ];
    }

    private function isFinishedGeneratingOutput(string $output, string $tailHash): ?string
    {
        $output = $this->isResultOutput($output) ?? $this->isErrorOutput($output);

        if (is_null($output)) {
            return null;
        }

        return !$this->isTailOutput($output, $tailHash) ? $output : '';
    }

    private function isResultOutput(string $output): ?string
    {
        $isMatch = (bool) preg_match_all(static::REGEX_RESULT_PATTERN, $output, $matches);

        if (!$isMatch) {
            return null;
        }

        return $matches[1][0];
    }

    private function isErrorOutput(string $output): ?string
    {
        $isMatch = (bool) preg_match_all(static::REGEX_ERROR_PATTERN, $output, $matches);

        if (!$isMatch) {
            return null;
        }

        return $matches[1][0];
    }

    private function isTailOutput(string $output, string $tailHash): bool
    {
        return (bool) preg_match_all(
            sprintf(
                static::REGEX_TAIL_PATTERN,
                $tailHash
            ),
            $output
        );
    }

    public function __destruct()
    {
        foreach ($this->pipes as $pipe) {
            fclose($pipe);
        }

        proc_terminate($this->process);
        proc_close($this->process);
    }
}
