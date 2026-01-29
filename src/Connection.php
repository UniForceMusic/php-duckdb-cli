<?php

namespace UniForceMusic\PHPDuckDBCLI;

use UniForceMusic\PHPDuckDBCLI\Enums\ModeEnum;
use UniForceMusic\PHPDuckDBCLI\Enums\PipesEnum;
use UniForceMusic\PHPDuckDBCLI\Exceptions\ConnectionException;
use UniForceMusic\PHPDuckDBCLI\Exceptions\DuckDBException;

class Connection
{
    private mixed $process;
    private array $pipes = [];
    private ?int $timeout = null;

    public function __construct(string $binary, ?string $file, private ModeEnum $mode)
    {
        $command = sprintf(
            '%s -noheader',
            escapeshellarg($binary)
        );

        if ($mode == ModeEnum::JSON) {
            $command .= ' -json';
        }

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

    public function removeTimeout(): void
    {
        $this->timeout = null;
    }

    public function setTimeout(int $microseconds): void
    {
        $this->timeout = $microseconds;
    }

    public function changeMode(ModeEnum $mode): void
    {
        $this->mode = $mode;

        if ($mode == ModeEnum::JSON) {
            $this->execute('.mode json', false, false);
        } else {
            $this->execute('.mode duckbox', false, false);
        }
    }

    public function execute(string $sql, bool $addSemicolon = true, bool $expectResult = true): ?Result
    {
        $startTime = microtime(true);

        while (true) {
            $this->throwExceptionIfTimedOut(
                $startTime,
                'waiting for remaining output to finish streaming'
            );

            if (empty($this->readStreams())) {
                break;
            }
        }

        if ($addSemicolon && !preg_match('/;\s*$/', $sql)) {
            $sql .= ';';
        }

        fwrite($this->pipes[PipesEnum::STDIN->value], $sql . PHP_EOL);

        if (!$expectResult) {
            return null;
        }

        $output = '';

        while (true) {
            $this->throwExceptionIfTimedOut(
                $startTime,
                "executing '{$sql}'"
            );

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
        }

        return new Result($output, $this->mode);
    }

    private function readStreams(): string
    {
        return (string) stream_get_contents($this->pipes[PipesEnum::STDOUT->value])
            . (string) stream_get_contents($this->pipes[PipesEnum::STDERR->value]);
    }

    private function throwExceptionIfTimedOut(float $startTime, string $reason): void
    {
        if (!$this->timeout) {
            return;
        }

        if ((microtime(true) - $startTime) * 10000 > $this->timeout) {
            throw new ConnectionException("connection timed out while {$reason}");
        }
    }

    private function hasFinishedGeneratingOutput(string $output): array
    {
        if ((bool) preg_match('/^([A-Za-z\-\_\s]*Error\:?[\S\s]+)$/im', $output, $match)) {
            return [false, $match[1]];
        }

        if ((bool) preg_match('/changes\:\s*[0-9]+\s*total_changes\:\s*[0-9]+\s*$/', $output)) {
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
