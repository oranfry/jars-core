<?php

namespace jars\linetype;

class token extends \jars\Linetype
{
    function __construct()
    {
        $this->table = 'token';

        $this->fields = [
            'token' => fn ($records): ?string => ($base64 = $records['/']->token) ? bin2hex(base64_decode($base64)) : null,
            'ttl' => fn ($records): ?int => $records['/']->ttl,
            'used' => fn ($records): int => $records['/']->used,
            'expired' => fn ($records): bool => strtotime($records['/']->used) + $records['/']->ttl > time(),
        ];

        $this->unfuse_fields = [
            'token' => fn ($line): ?string => (false !== $bin = @hex2bin(@$line->token)) ? base64_encode($bin) : null,
            'ttl' => fn ($line, $oldline): ?int => is_numeric(@$line->ttl) ? (int) $line->ttl : 0,
            'used' => fn ($line, $oldline): int => time(),
        ];
    }

    function complete($line): void
    {
        $line->ttl = $line->ttl ?? 86400;
    }
}
