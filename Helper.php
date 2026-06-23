<?php

namespace OranFry\Jars\Core;

class Helper
{
    public static function file_put_contents(string $filename, string $data, int $flags = 0): void
    {
        $tempfile = $filename . '.tmp' . bin2hex(random_bytes(8));
        $generateErrorMessage = fn ($num) => "Could not file_put_contents [$filename] ($num)";

        $mode = 'w';

        if ($flags & FILE_APPEND) {
            $mode = 'a';

            if (is_file($filename) && !copy($filename, $tempfile)) {
                throw new Exception($generateErrorMessage(1));
            }
        }

        if (!$handle = fopen($tempfile, $mode)) {
            throw new Exception($generateErrorMessage(2));
        }

        if (fwrite($handle, $data) === false) {
            throw new Exception($generateErrorMessage(3));
        }

        if (!fsync($handle)) {
            throw new Exception($generateErrorMessage(4));
        }

        if (!fclose($handle)) {
            throw new Exception($generateErrorMessage(5));
        }

        if (!rename($tempfile, $filename)) {
            throw new Exception($generateErrorMessage(6));
        }
    }
}