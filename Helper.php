<?php

namespace OranFry\Jars\Core;

use OranFry\Jars\Contract\Exception;

class Helper
{
    public static function mkdir(string $dir, string $base): void
    {
        $create = [];

        for (
            $path = $dir;
            strlen($path) > 1 && $path !== $base;
            $path = dirname($path)
        ) {
            array_unshift($create, $path);
            $prev = $path;
        }

        array_unshift($create, $base);

        if ($path !== $base) {
            throw new Exception("Block: was unable to track block path back to chain path");
        }

        array_walk($create, fn ($dir) => @mkdir($dir));
    }

    public static function file_put_contents(string $filename, $data): void
    {
        $tempfile = $filename . '.tmp' . bin2hex(random_bytes(8));
        $handle = fopen($tempfile, 'w');
        $generateErrorMessage = fn ($num) => "Could not file_put_contents [$filename] ($num)";

        if (!$handle = fopen($tempfile, 'w')) {
            throw new Exception($generateErrorMessage(1));
        }

        if (fwrite($handle, $data) === false) {
            throw new Exception($generateErrorMessage(2));
        }

        if (!fsync($handle)) {
            throw new Exception($generateErrorMessage(3));
        }

        if (!fclose($handle)) {
            throw new Exception($generateErrorMessage(4));
        }

        if (!rename($tempfile, $filename)) {
            throw new Exception($generateErrorMessage(5));
        }
    }
}
