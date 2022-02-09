<?php

namespace jars;

class Jars implements Client
{
    private static $head;
    private static $version_number;
    public static $verified_tokens = [];

    private $db_home;
    private $portal_home;
    private $token;

    function __construct($auth = null)
    {
    }

    public static function validate_username(string $username)
    {
        return
            is_string($username)
            &&
            strlen($username) > 0
            &&
            (
                preg_match('/^[a-z0-9_]+$/', $username)
                ||
                filter_var($username, FILTER_VALIDATE_EMAIL) !== false
            )
            ;
    }

    public static function validate_password(string $password)
    {
        return
            is_string($password)
            &&
            strlen($password) > 5
            ;
    }

    public static function login(?Filesystem $filesystem, string $username, string $password, bool $one_time = false)
    {
        if (!$one_time && is_null($filesystem)) {
            error_response('Filesystem is required unless creating one-time login');
        }

        if (!static::validate_username($username)) {
            error_response('Invalid username');
        }

        if (!static::validate_password($password)) {
            error_response('Invalid password');
        }

        if (($root_username = @BlendsConfig::get()->root_username) && $username == $root_username) {
            if (!($root_password = @BlendsConfig::get()->root_password)) {
                error_response('Root username is set up without a root password');
            }

            if ($password !== $root_password) {
                return null;
            }
        }

        $random_bits = bin2hex(openssl_random_pseudo_bytes(32));

        $line = (object) [
            'token' => $random_bits,
            'ttl' => 86400,
            'type' => 'token',
            'used' => time(),
        ];

        static::$verified_tokens[$random_bits] = $line;

        if ($one_time) {
            return $random_bits;
        }

        // convert token from one-time to persistent

        list($line) = static::save($random_bits, $filesystem, [$line]);
        unset(static::$verified_tokens[$random_bits]);

        $token = $line->id . ':' . $random_bits;

        static::$verified_tokens[$token] = $line;

        return $token;
    }

    public static function token_user(string $token, Filesystem $filesystem)
    {
        if (!static::verify_token($token, $filesystem)) {
            return;
        }

        return static::$verified_tokens[$token]->user;
    }

    public static function token_username(string $token, Filesystem $filesystem)
    {
        if (!static::verify_token($token, $filesystem)) {
            return;
        }

        return static::$verified_tokens[$token]->username ?? @BlendsConfig::get()->root_username;
    }

    public static function verify_token(string $token, Filesystem $filesystem)
    {
        if (!$db_home = @Config::get()->db_home) {
            error_response('db_home not defined', 500);
        }

        if (isset(static::$verified_tokens[$token])) {
            return true;
        }

        if (!preg_match('/^([a-zA-Z0-9]+):([0-9a-f]{64})$/', $token, $groups)) {
            return false;
        }

        // token deleted or never existed

        list (, $id, $random_bits) = $groups;

        $time = microtime(true);
        $line = Linetype::load(null, $filesystem, 'token')->get(null, $filesystem, $id);

        if (
            !$line
            ||
            $line->token !== $random_bits
            ||
            $line->ttl + $line->used < time()
        ) {
            usleep((0.5 + $time - microtime(true)) * 1000000); // don't let on whether the line existed

            return false;
        }

        $user = null;

        if (@$line->user) {
            $userfile = $db_home . '/current/records/users/' . $line->user . '.json';

            if (!$filesystem->has($userfile)) {
                error_response('Token Verification Error 1', 500);
            }

            $user = json_decode($filesystem->get($userfile));
        }

        $token_object = (object) [
            'token' => $line->token,
            'user' => @$user->id,
            'username' => @$user->username ?? @BlendsConfig::get()->root_username
        ];

        static::$verified_tokens[$token] = $token_object;

        return true;
    }

    public static function logout(string $token, Filesystem $filesystem)
    {
        if (!static::verify_token($token, $filesystem)) {
            return;
        }

        $line = (object)['token' => $token, 'type' => 'token', '_is' => false];
        Blends::import($token, $filesystem, date('Y-m-d H:i:s'), [$line]);
    }

    public static function import(string $token, Filesystem $filesystem, string $timestamp, array $lines, bool $dryrun = false, ?int $logging = null)
    {
        $affecteds = [];
        $commits = [];
        $original_filesystem = clone $filesystem;
        $original_filesystem->freeze();
        $lines = static::import_r($token, $filesystem, $original_filesystem, $timestamp, $lines, $affecteds, $commits, null, $logging);
        $meta  = [];

        if (!$db_home = @Config::get()->db_home) {
            error_response('db_home not defined', 500);
        }

        if (!$dryrun && file_exists($current_version_file = $db_home . '/version.dat')) {
            $version = trim(file_get_contents($current_version_file));

            `mkdir -p "$db_home/past" && cp -r "$db_home/current" "$db_home/past/$version"`;
        }

        foreach ($affecteds as $affected) {
            switch ($affected->action) {
                case 'connect':
                    (new Link($filesystem, $affected->tablelink, $affected->left))
                        ->add($affected->right)
                        ->save();

                    (new Link($filesystem, $affected->tablelink, $affected->right, true))
                        ->add($affected->left)
                        ->save();
                    break;

                case 'delete':
                    $affected->oldrecord->delete();
                    $meta[] = '-' . $affected->table . ':' . $affected->id;
                    break;

                case 'disconnect':
                    (new Link($filesystem, $affected->tablelink, $affected->left))
                        ->remove($affected->right)
                        ->save();

                    (new Link($filesystem, $affected->tablelink, $affected->right, true))
                        ->remove($affected->left)
                        ->save();
                    break;

                case 'save':
                    $affected->record->save();
                    $meta[] = ($affected->oldrecord ? '~' : '+') . $affected->table . ':' . $affected->id;
                    break;

                default:
                    error_response('Unknown action: ' . @$affected->action);
            }
        }

        if ($dryrun) {
            $updated = [];

            foreach ($lines as $line) {
                $updated[] = Linetype::load($token, $filesystem, $line->type)->get($token, $filesystem, $line->id);
            }

            $filesystem->revert();

            return $updated;
        }

        $commits = array_filter($commits);

        static::commit($token, $filesystem, $timestamp, $commits, $meta);

        if (defined('BLENDS_PERSIST_PER_IMPORT') && BLENDS_PERSIST_PER_IMPORT) {
            $filesystem->persist();
        }

        return $lines;
    }

    public static function import_r(string $token, Filesystem $filesystem, Filesystem $original_filesystem, string $timestamp, array $lines, array &$affecteds, array &$commits, ?string $ignorelink = null, ?int $logging = null)
    {
        $config = BlendsConfig::get($token, $filesystem);

        foreach ($lines as $line) {
            if (!is_object($line)) {
                error_response('Lines should be an array of objects');
            } elseif (!property_exists($line, 'type')) {
                error_response('All lines must have a type');
            } elseif (!array_key_exists($line->type, $config->linetypes)) {
                error_response('Unrecognised linetype: ' . $line->type);
            }
        }

        foreach ($lines as $line) {
            Linetype::load($token, $filesystem, $line->type)->import($token, $filesystem, $original_filesystem, $timestamp, $line, $affecteds, $commits, $ignorelink, $logging);
        }

        foreach ($lines as $line) {
            Linetype::load($token, $filesystem, $line->type)->recurse_to_children($token, $filesystem, $original_filesystem, $timestamp, $line, $affecteds, $commits, $ignorelink, $logging);
        }

        return $lines;
    }

    public static function complete(string $token, Filesystem $filesystem, array $lines) : void
    {
        if (!Blends::verify_token($token, $filesystem)) {
            return;
        }

        foreach ($lines as $line) {
            Linetype::load($token, $filesystem, $line->type)->complete($line);
        }
    }

    public static function save(string $token, Filesystem $filesystem, array $lines, bool $dryrun = false)
    {
        if (!$lines) {
            return $lines;
        }

        if (!Blends::verify_token($token, $filesystem)) {
            return false;
        }

        return static::import($token, $filesystem, date('Y-m-d H:i:s'), $lines, $dryrun);
    }

    private static function commit(string $token, Filesystem $filesystem, $timestamp, array $data, array $meta)
    {
        if (!$db_home = @Config::get()->db_home) {
            error_response('db_home not defined', 500);
        }

        foreach ($data as $id => $commit) {
            if (!count(array_diff(array_keys(get_object_vars($commit)), ['id', 'type']))) {
                unset($data[$id]);
            }
        }

        if (!count($data)) {
            return;
        }

        if (!is_dir($version_dir = $db_home . '/versions')) {
            mkdir($version_dir, 0777, true);
        }

        $master_record_file = $db_home . '/master.dat';
        $master_meta_file = $master_record_file . '.meta';
        $current_version_file = $db_home . '/version.dat';

        if (static::$head === null) {
            if (file_exists($current_version_file)) {
                static::$head = $filesystem->get($current_version_file);
                static::$version_number = (int) $filesystem->get($version_dir . '/' . static::$head);
            } else {
                static::$head = hash('sha256', 'jars'); // version 0
                static::$version_number = 0;
            }
        }

        // Db::succeed('select counter from master_record_lock for update');

        $master_export = $timestamp . ' ' . json_encode(array_values($data));
        $meta_export = implode(' ', $meta);

        static::$head = hash('sha256', static::$head . $master_export);
        static::$version_number++;

        $filesystem->donefile($db_home . '/touch.dat');
        $filesystem->put($current_version_file, static::$head);
        $filesystem->put($version_dir . '/' . static::$head, static::$version_number);
        $filesystem->append($master_meta_file, static::$head . ' ' . $meta_export . "\n");
        $filesystem->append($master_record_file, static::$head . ' ' . $master_export . "\n");

        // Db::succeed('update master_record_lock set counter = counter + 1');
    }

    private function head($filesystem)
    {
        if (!$db_home = @Config::get()->db_home) {
            error_response('db_home not defined', 500);
        }

        $master_record_file = $db_home . '/master.dat';
        $master_record = $filesystem->get($master_record_file);
        $head = substr($master_record, strrpos($master_record, "\n") ?: 0, 64);
    }

    public function h2n($h)
    {
        $sequence = BlendsConfig::get()->sequence;

        for ($n = 1; $n <= $sequence->max; $n++) {
            if (n2h($n) == $h) {
                return $n;
            }
        }
    }

    public function n2h($n)
    {
        // Generate a sequence secret: php -r 'echo base64_encode(random_bytes(32)) . "\n";'
        $sequence = BlendsConfig::get()->sequence;

        $banned = @$sequence->banned_chars ?? [];
        $replace = array_fill(0, count($banned), '');
        $sequence_secret = @$sequence->secret;
        $subs = @$sequence->subs ?? [];

        if (!$sequence_secret) {
            error_response('Sequence Secret not defined');
        }

        if (strlen($sequence_secret) < 8) {
            error_response('Sequence Secret too weak (8-char minimum)');
        }

        if (isset($subs[$n])) {
            return $subs[$n];
        }

        return strtoupper(substr(str_replace($banned, $replace, base64_encode(hex2bin(hash('sha256', $n . '--' . $sequence_secret)))), 0, 10));
    }

    public function masterlog_check()
    {
        if (!$db_home = @Config::get()->db_home) {
            error_response('db_home not defined', 500);
        }

        $master_record_file = $db_home . '/master.dat';
        $master_meta_file = $master_record_file . '.meta';

        if (!touch($master_record_file) || !is_writable($master_record_file)) {
            error_response('Master record file not writable');
        }

        if (!touch($master_meta_file) || !is_writable($master_meta_file)) {
            error_response('Master meta file not writable');
        }
    }
}
