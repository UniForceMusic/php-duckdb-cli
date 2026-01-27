<?php

namespace UniForceMusic\PHPDuckDBCLI;

use UniForceMusic\PHPDuckDBCLI\Exceptions\ConnectionException;
use UniForceMusic\PHPDuckDBCLI\Exceptions\DuckDBException;

class Connection
{
    public const int PIPE_STDIN = 0;
    public const int PIPE_STDOUT = 1;
    public const int PIPE_STDERR = 2;
    public const int USLEEP_TIME = 100;
    public const string REGEX_CHANGES_PATTERN = '/changes\:\s*[0-9]+\s*total_changes\:\s*[0-9]+\s*$/';
    public const string REGEX_ERROR_PATTERN = '/^([A-Za-z\-\_\s]*Error\:?[\S\s]+)$/im';

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

    public function execute(string $sql, bool $addSemicolon = true, bool $expectResult = true): ?Result
    {
        $this->readStreams();

        if ($addSemicolon && !preg_match('/;\s*$/', $sql)) {
            $sql .= ';';
        }

        fwrite($this->pipes[self::PIPE_STDIN], $sql);
        fwrite($this->pipes[self::PIPE_STDIN], PHP_EOL);

        if (!$expectResult) {
            return null;
        }

        $output = '';

        while (true) {
            $output .= $this->readStreams();

            if (!proc_get_status($this->process)['running']) {
                throw new DuckDBException('DuckDB process has quit unexpectedly');
            }

            [$hasChangesOutput, $errorOutput] = $this->hasFinishedGeneratingOutput($output);

            if ($hasChangesOutput) {
                break;
            }

            if (!empty($errorOutput)) {
                throw new DuckDBException($errorOutput);
            }

            usleep(static::USLEEP_TIME);
        }

        return new Result($output);
    }

    private function readStreams(): string
    {
        return (string) stream_get_contents($this->pipes[self::PIPE_STDOUT])
            . (string) stream_get_contents($this->pipes[self::PIPE_STDERR]);
    }

    private function hasFinishedGeneratingOutput(string $output): array
    {
        if ((bool) preg_match_all(static::REGEX_ERROR_PATTERN, $output, $matches)) {
            return [false, $matches[1][0]];
        }

        if ((bool) preg_match_all(static::REGEX_CHANGES_PATTERN, $output, $matches)) {
            return [true, false];
        }

        return [false, false];
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
