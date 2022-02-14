<?php

namespace jars\linetype;

use Exception;

class token extends \jars\Linetype
{
    function __construct()
    {
        $this->table = 'token';
        $this->fields = [
            'token' => function($records) : string {
                return $records['/']->token;
            },
            'ttl' => function($records) : int {
                return $records['/']->ttl;
            },
            'used' => function($records) : int {
                if (!is_int($records['/']->used)) {
                    throw new Exception('"used" not an int: ' . var_export($record, 1));
                }
                return $records['/']->used;
            },
            'hits' => function($records) : int {
                return $records['/']->hits;
            },
            'expired' => function($records) : bool {
                return strtotime($records['/']->used) + $records['/']->ttl > time();
            },
        ];
        $this->unfuse_fields = [
            'token' => function($line, $oldline) : string {
                return $line->token;
            },
            'ttl' => function($line, $oldline) : int {
                return $line->ttl;
            },
            'used' => function($line, $oldline) : int {
                return time();
            },
            'hits' => function($line, $oldline) : int {
                return @$line->hits ?? 0;
            },
        ];
    }

    function complete($line) : void
    {
        $line->ttl = @$line->ttl ?? 86400;
        $line->hits = @$line->hits ?? 0;
    }
}
