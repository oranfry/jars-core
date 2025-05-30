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
            'expired' => fn ($records): bool => time() > strtotime($records['/']->used) + $records['/']->ttl,
            'delete_protected' => fn ($records): bool => $records['/']->delete_protected ?? false,
        ];

        $this->unfuse_fields = [
            'token' => fn ($line): ?string => (false !== $bin = @hex2bin($line->token)) ? base64_encode($bin) : null,
            'ttl' => fn ($line, $oldline): ?int => is_numeric(@$line->ttl) ? (int) $line->ttl : 0,
            'used' => fn ($line, $oldline): int => time(),
            'delete_protected' => fn ($line, $oldline): bool => (bool) $line->delete_protected,
        ];
    }

    function complete($line): void
    {
        $line->ttl ??= 86400;
        $line->delete_protected ??= false;
    }

    public function validate($line): array
    {
        $is = $line->_is ?? true;

        if (!$is && $line->delete_protected) {
            return ['You cannot delete a protected token'];
        }

        return parent::validate($line);
    }
}
