<?php

namespace jars;

class BlendsConfig
{
    private static $known = [];

    public function get($token = null, ?Filesystem $filesystem = null)
    {
        if (isset(static::$known[$token])) {
            return static::$known[$token];
        }

        $entrypoint = 'base';

        if ($token && !$filesystem) {
            error_response(__METHOD__ . ': Filesystem must be given if token given');
        }

        if ($token && Blends::verify_token($token, $filesystem)) {
            $entrypoint = 'public';

            if (
                ($root_username = BlendsConfig::get()->root_username)
                &&
                Blends::token_username($token, $filesystem) == $root_username
            ) {
                $entrypoint = 'root';
            }
        }

        $subsimple_config = Config::get();

        if (!property_exists($subsimple_config, 'entrypoints')) {
            error_response('Entrypoints not defined');
        }

        if (!isset($subsimple_config->entrypoints[$entrypoint])) {
            error_response("Entrypoint {$entrypoint} not defined");
        }

        $config_class = $subsimple_config->entrypoints[$entrypoint];
        $config = new $config_class();

        foreach (['linetypes', 'tables', 'reports'] as $listname) {
            if (!property_exists($config, $listname)) {
                $config->{$listname} = [];
            }
        }

        if (!in_array('user', array_keys($config->linetypes))) {
            $config->linetypes['user'] = (object) [
                'canwrite' => true,
                'class' => 'blends\\linetype\\user',
            ];
        }

        if (!in_array('token', array_keys($config->linetypes))) {
            $config->linetypes['token'] = (object) [
                'cancreate' => true,
                'canwrite' => true,
                'candelete' => true,
                'class' => 'blends\\linetype\\token',
            ];
        }

        static::$lookup[$token] = $config;

        return $config;
    }
}
