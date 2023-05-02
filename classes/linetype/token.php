<?php

namespace jars\linetype;

class token extends \jars\Linetype
{
    function __construct()
    {
        $this->table = 'token';

        $this->fields = [
            'token' => fn ($records): string => $records['/']->token,
            'ttl' => fn ($records): int => $records['/']->ttl,
            'used' => fn ($records): int => $records['/']->used,
            'hits' => fn ($records): int => $records['/']->hits,
            'expired' => fn ($records): bool => strtotime($records['/']->used) + $records['/']->ttl > time(),
        ];

        $this->unfuse_fields = [
            'token' => fn ($line, $oldline): string => $line->token,
            'ttl' => fn ($line, $oldline): int => $line->ttl,
            'used' => fn ($line, $oldline): int => time(),
            'hits' => fn ($line, $oldline): int => @$line->hits ?? 0,
        ];
    }

    function complete($line) : void
    {
        $line->ttl = @$line->ttl ?? 86400;
        $line->hits = @$line->hits ?? 0;
    }
}
