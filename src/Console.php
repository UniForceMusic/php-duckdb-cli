<?php

namespace Sentience\Helpers;

class Console
{
    public const int DEFAULT_WIDTH = 80;

    protected static ?int $width = null;

    public static function exec(string $command, &$stdout = null, &$stderr = null, float $interval = 1000): int
    {
        $pipes = [];

        $process = proc_open(
            $command,
            [
                ['pipe', 'r'],
                ['pipe', 'w'],
                ['pipe', 'w']
            ],
            $pipes
        );

        $stdout = '';
        $stderr = '';

        if (!is_resource($process)) {
            return 1;
        }

        stream_set_blocking($pipes[0], false);
        stream_set_blocking($pipes[1], false);
        stream_set_blocking($pipes[2], false);

        fwrite($pipes[0], '');
        fclose($pipes[0]);

        while (is_resource($process)) {
            $stdout .= stream_get_contents($pipes[1]);
            $stderr .= stream_get_contents($pipes[2]);

            $status = proc_get_status($process);

            if (!$status['running']) {
                fclose($pipes[1]);
                fclose($pipes[2]);

                proc_close($process);

                return (int) $status['exitcode'];
            }

            usleep((int) $interval * 1000);
        }

        return 1;
    }

    public static function stream(string $command, callable $callback, float $interval = 1000): int
    {
        $pipes = [];

        $process = proc_open(
            $command,
            [
                ['pipe', 'r'],
                ['pipe', 'w'],
                ['pipe', 'w']
            ],
            $pipes
        );

        $stdout = '';
        $stderr = '';

        if (!is_resource($process)) {
            return 1;
        }

        stream_set_blocking($pipes[0], false);
        stream_set_blocking($pipes[1], false);
        stream_set_blocking($pipes[2], false);

        fwrite($pipes[0], '');

        while (is_resource($process)) {
            $stdout = stream_get_contents($pipes[1]);
            $stderr = stream_get_contents($pipes[2]);

            $stdin = $callback($stdout, $stderr);

            if (is_int($stdin)) {
                fclose($pipes[0]);
                fclose($pipes[1]);
                fclose($pipes[2]);

                proc_terminate($process);

                return (int) $stdin;
            }

            if (is_string($stdin)) {
                fwrite($pipes[0], $stdin);
            }

            $status = proc_get_status($process);

            if (!$status['running']) {
                fclose($pipes[0]);
                fclose($pipes[1]);
                fclose($pipes[2]);

                proc_close($process);

                return (int) $status['exitcode'];
            }

            usleep((int) $interval * 1000);
        }

        return 1;
    }

    public static function getWidth(bool $useCache = true): int
    {
        if ($useCache && static::$width) {
            return static::$width;
        }

        $width = PHP_OS_FAMILY == 'Windows'
            ? static::getWidthCommandPrompt()
            : static::getWidthTerminal();

        static::$width = $width;

        return $width;
    }

    protected static function getWidthCommandPrompt(): int
    {
        $exitCode = static::exec('cmd /c mode con', $stdout, $stderr, 0);

        if ($exitCode != 0) {
            return static::DEFAULT_WIDTH;
        }

        $isMatch = preg_match('/Columns\:\s+([0-9]+)/', (string) $stdout, $matches);

        if (!$isMatch) {
            return static::DEFAULT_WIDTH;
        }

        return (int) $matches[1];
    }

    protected static function getWidthTerminal(): int
    {
        $exitCode = static::exec('stty size < /dev/tty', $stdout, $stderr, 0);

        if (preg_match('/(\d+)\s(\d+)/', (string) $stdout, $matches)) {
            [$height, $width] = array_slice($matches, 1);

            return (int) $width;
        }

        $exitCode = static::exec('tput cols', $stdout, $stderr, 0);

        if ($exitCode == 0) {
            return (int) $stdout;
        }

        return static::DEFAULT_WIDTH;
    }
}
