<?php

namespace jars\linetype;

class token extends \jars\Linetype
{
    function __construct()
    {
        $this->table = 'token';

        $this->simple_hex('token');
        $this->simple_int('ttl');
        $this->simple_int('hits', 0);

        $this->fields['used'] = fn ($records): int => $records['/']->used;
        $this->unfuse_fields['used'] = fn ($line, $oldline): int => time();

        $this->fields['expired'] = fn ($records): bool => strtotime($records['/']->used) + $records['/']->ttl > time();
    }

    function complete($line): void
    {
        $line->ttl = $line->ttl ?? 86400;
    }
}
