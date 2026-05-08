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
}