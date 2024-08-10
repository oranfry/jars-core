<?php

namespace jars;

use jars\contract\Exception;

class Sequence
{
    protected string $secret;

    public function __construct(string $encoded)
    {
        // Generate a sequence secret:
        // php -r "echo base64_encode(implode('', array_map(fn () => chr(random_int(0, 33)), array_fill(0, 66, 0)))) . PHP_EOL;"

        $binary = base64_decode($encoded);

        if ($binary === false) {
            throw new Exception('Sequence Secret should be base64 encoded');
        }

        if (strlen($binary) < 32) {
            throw new Exception('Sequence Secret too weak (32-byte minimum)');
        }

        $this->secret = $binary;
    }

    public function secret(): String
    {
        return $this->secret;
    }
}
