<?php

namespace jars;

use Exception;

class Jars implements contract\Client
{
    private $db_home;
    private $filesystem;
    private $head;
    private $known = [];
    private $portal_home;
    private $token;
    private $verified_tokens = [];
    private $version_number;

    public function __construct(string $portal_home, string $db_home)
    {
        $this->db_home = $db_home;
        $this->portal_home = $portal_home;

        $this->filesystem = new Filesystem();
    }

    public function __clone()
    {
        $this->filesystem = clone $this->filesystem;
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

    public function login(string $username, string $password, bool $one_time = false)
    {
        if (!$this->validate_username($username)) {
            throw new Exception('Invalid username');
        }

        if (!$this->validate_password($password)) {
            throw new Exception('Invalid password');
        }

        $config = $this->config();

        if (($root_username = $config->root_username) && $username == $root_username) {
            if (!($root_password = $config->root_password)) {
                throw new Exception('Root username is set up without a root password');
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

        $this->verified_tokens[$random_bits] = $line;

        $this->token = $random_bits;

        if ($one_time) {
            return $this->token;
        }

        // convert token from one-time to persistent

        list($line) = $this->save([$line]);
        unset($this->verified_tokens[$random_bits]);

        $this->verified_tokens[$this->token = $line->id . ':' . $random_bits] = $line;

        return $this->token;
    }

    public function token_user(string $token)
    {
        if (!$this->verify_token($token)) {
            return;
        }

        return $this->verified_tokens[$token]->user;
    }

    public function token_username(string $token)
    {
        if (!$this->verify_token($token)) {
            return;
        }

        return $this->verified_tokens[$token]->username ?? $this->config()->root_username;
    }

    public function verify_token(string $token)
    {
        if (isset($this->verified_tokens[$token])) {
            return true;
        }

        if (!preg_match('/^([a-zA-Z0-9]+):([0-9a-f]{64})$/', $token, $groups)) {
            return false;
        }

        // token deleted or never existed

        list (, $id, $random_bits) = $groups;

        $time = microtime(true);
        $line = $this->linetype('token', true)->get(null, $id);

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
            $userfile = $this->db_home . '/current/records/users/' . $line->user . '.json';

            if (!$this->filesystem->has($userfile)) {
                throw new Exception('Token Verification Error 1', 500);
            }

            $user = json_decode($this->filesystem->get($userfile));
        }

        $token_object = (object) [
            'token' => $line->token,
            'user' => @$user->id,
            'username' => @$user->username ?? $this->config()->root_username
        ];

        $this->verified_tokens[$token] = $token_object;

        return true;
    }

    public function logout()
    {
        if (!$this->verify_token($this->token)) {
            return;
        }

        $line = (object)['token' => $this->token, 'type' => 'token', '_is' => false];
        $this->import(date('Y-m-d H:i:s'), [$line]);
    }

    public function import(string $timestamp, array $lines, bool $dryrun = false, ?int $logging = null)
    {
        $affecteds = [];
        $commits = [];
        $original_filesystem = clone $this->filesystem;
        $original_filesystem->freeze();
        $lines = $this->import_r($original_filesystem, $timestamp, $lines, $affecteds, $commits, null, $logging);
        $meta  = [];

        if (!$dryrun && file_exists($current_version_file = $this->db_home . '/version.dat')) {
            $version = trim(file_get_contents($current_version_file));

            `mkdir -p "{$this->db_home}/past" && cp -r "{$this->db_home}/current" "{$this->db_home}/past/$version"`;
        }

        foreach ($affecteds as $affected) {
            switch ($affected->action) {
                case 'connect':
                    Link::of($this, $affected->tablelink, $affected->left)
                        ->add($affected->right)
                        ->save();

                    Link::of($this, $affected->tablelink, $affected->right, true)
                        ->add($affected->left)
                        ->save();
                    break;

                case 'delete':
                    $affected->oldrecord->delete();
                    $meta[] = '-' . $affected->table . ':' . $affected->id;
                    break;

                case 'disconnect':
                    Link::of($this, $affected->tablelink, $affected->left)
                        ->remove($affected->right)
                        ->save();

                    Link::of($this, $affected->tablelink, $affected->right, true)
                        ->remove($affected->left)
                        ->save();
                    break;

                case 'save':
                    $affected->record->save();
                    $meta[] = ($affected->oldrecord ? '~' : '+') . $affected->table . ':' . $affected->id;
                    break;

                default:
                    throw new Exception('Unknown action: ' . @$affected->action);
            }
        }

        if ($dryrun) {
            $updated = [];

            foreach ($lines as $line) {
                $updated[] = $this->linetype($line->type)->get($this->token, $line->id);
            }

            $this->filesystem->revert();

            return $updated;
        }

        $commits = array_filter($commits);

        $this->commit($timestamp, $commits, $meta);

        if (defined('BLENDS_PERSIST_PER_IMPORT') && BLENDS_PERSIST_PER_IMPORT) {
            $this->filesystem->persist();
        }

        return $lines;
    }

    public function import_r(Filesystem $original_filesystem, string $timestamp, array $lines, array &$affecteds, array &$commits, ?string $ignorelink = null, ?int $logging = null)
    {
        $config = $this->config();

        foreach ($lines as $line) {
            if (!is_object($line)) {
                throw new Exception('Lines should be an array of objects');
            } elseif (!property_exists($line, 'type')) {
                throw new Exception('All lines must have a type');
            } elseif (!array_key_exists($line->type, $config->linetypes)) {
                throw new Exception('Unrecognised linetype: ' . $line->type);
            }
        }

        foreach ($lines as $line) {
            $this->linetype($line->type)->import($this->token, $original_filesystem, $timestamp, $line, $affecteds, $commits, $ignorelink, $logging);
        }

        foreach ($lines as $line) {
            $this->linetype($line->type)->recurse_to_children($this->token, $original_filesystem, $timestamp, $line, $affecteds, $commits, $ignorelink, $logging);
        }

        return $lines;
    }

    public function save(array $lines)
    {
        if (!$lines) {
            return $lines;
        }

        return $this->import(date('Y-m-d H:i:s'), $lines);
    }

    private function commit($timestamp, array $data, array $meta)
    {
        foreach ($data as $id => $commit) {
            if (!count(array_diff(array_keys(get_object_vars($commit)), ['id', 'type']))) {
                unset($data[$id]);
            }
        }

        if (!count($data)) {
            return;
        }

        if (!is_dir($version_dir = $this->db_home . '/versions')) {
            mkdir($version_dir, 0777, true);
        }

        $master_record_file = $this->db_home . '/master.dat';
        $master_meta_file = $master_record_file . '.meta';
        $current_version_file = $this->db_home . '/version.dat';

        if ($this->head === null) {
            if (file_exists($current_version_file)) {
                $this->head = $this->filesystem->get($current_version_file);
                $this->version_number = (int) $this->filesystem->get($version_dir . '/' . $this->head);
            } else {
                $this->head = hash('sha256', 'jars'); // version 0
                $this->version_number = 0;
            }
        }

        // Db::succeed('select counter from master_record_lock for update');

        $master_export = $timestamp . ' ' . json_encode(array_values($data));
        $meta_export = implode(' ', $meta);

        $this->head = hash('sha256', $this->head . $master_export);
        $this->version_number++;

        $this->filesystem->donefile($this->db_home . '/touch.dat');
        $this->filesystem->put($current_version_file, $this->head);
        $this->filesystem->put($version_dir . '/' . $this->head, $this->version_number);
        $this->filesystem->append($master_meta_file, $this->head . ' ' . $meta_export . "\n");
        $this->filesystem->append($master_record_file, $this->head . ' ' . $master_export . "\n");

        // Db::succeed('update master_record_lock set counter = counter + 1');
    }

    private function head()
    {
        $master_record_file = $this->db_home . '/master.dat';
        $master_record = $this->filesystem->get($master_record_file);
        $head = substr($master_record, strrpos($master_record, "\n") ?: 0, 64);
    }

    public function h2n($h)
    {
        $sequence = $this->config()->sequence;

        for ($n = 1; $n <= $sequence->max; $n++) {
            if ($this->n2h($n) == $h) {
                return $n;
            }
        }
    }

    public function n2h($n)
    {
        // Generate a sequence secret: php -r 'echo base64_encode(random_bytes(32)) . "\n";'
        $sequence = $this->config()->sequence;

        $banned = @$sequence->banned_chars ?? [];
        $replace = array_fill(0, count($banned), '');
        $sequence_secret = @$sequence->secret;
        $subs = @$sequence->subs ?? [];

        if (!$sequence_secret) {
            throw new Exception('Sequence Secret not defined');
        }

        if (strlen($sequence_secret) < 8) {
            throw new Exception('Sequence Secret too weak (8-char minimum)');
        }

        if (isset($subs[$n])) {
            return $subs[$n];
        }

        return strtoupper(substr(str_replace($banned, $replace, base64_encode(hex2bin(hash('sha256', $n . '--' . $sequence_secret)))), 0, 10));
    }

    public function masterlog_check()
    {
        $master_record_file = $this->db_home . '/master.dat';
        $master_meta_file = $master_record_file . '.meta';

        if (!touch($master_record_file) || !is_writable($master_record_file)) {
            throw new Exception('Master record file not writable');
        }

        if (!touch($master_meta_file) || !is_writable($master_meta_file)) {
            throw new Exception('Master meta file not writable');
        }
    }

    public function delete($linetype, $id)
    {
        return $this->linetype($linetype)->delete($id);
    }

    public function fields($linetype)
    {
        return $this->linetype(LINETYPE_NAME)->fieldInfo();
    }

    public function get($linetype, $id)
    {
        $line = $this->linetype($linetype)->get($this->token, $id);

        if (!$line) {
            return null;
        }

        return $line;
    }

    public function preview(array $lines)
    {
        if (!$lines) {
            return $lines;
        }

        return $this->import(date('Y-m-d H:i:s'), $lines, true);
    }

    public function record($table, $id, &$content_type = null)
    {
        $tableinfo = @$this->config()->tables[$table];;
        $ext = @$tableinfo->extension ?? 'json';
        $suffix = $ext ? '.' . $ext : null;
        $content_type = @$tableinfo->content_type ?? 'application/json';

        if (!is_file($file = $this->db_path('current/records/' . $table . '/' . $id . $suffix))) {
            return null;
        }

        return file_get_contents($file);
    }

    public function group(string $report, string $group, ?string $min_version = null)
    {
        return $this->report($report)->get($group, $min_version);
    }

    public function groups(string $report, ?string $min_version = null)
    {
        return $this->report($report)->groups($min_version);
    }

    public function touch()
    {
        if (!$this->verify_token($this->token())) {
            throw new Exception('Invalid / Expired Token');
        }

        return (object) [
            'timestamp' => time(),
        ];
    }

    public function version()
    {
        return $this->head;
    }

    public function config()
    {
        if (!isset($this->known['configs'][$this->portal_home])) {
            $config = require $this->portal_home . '/portal.php';

            foreach (['linetypes', 'tables', 'reports'] as $listname) {
                if (!property_exists($config, $listname)) {
                    $config->{$listname} = [];
                }
            }

            if (!in_array('token', array_keys($config->linetypes))) {
                $config->linetypes['token'] = (object) [
                    'cancreate' => true,
                    'canwrite' => true,
                    'candelete' => true,
                    'class' => 'jars\\linetype\\token',
                ];
            }

            $this->known['configs'][$this->portal_home] = $config;
        }

        return $this->known['configs'][$this->portal_home];
    }

    public function linetype(string $name, bool $from_base_config = false)
    {
        if (!isset($this->known['linetypes'][$name])) {
            $linetypeclass = $this->config($from_base_config)->linetypes[$name]->class;

            if (!$linetypeclass) {
                throw new Exception("No such linetype '{$name}'");
            }

            $linetype = new $linetypeclass();
            $linetype->name = $name;

            $linetype->jars($this);
            $linetype->filesystem($this->filesystem());

            if (method_exists($linetype, 'init')) {
                $linetype->init($token);
            }

            $this->known['linetypes'][$name] = $linetype;
        }

        return $this->known['linetypes'][$name];
    }

    public function report(string $name)
    {
        if (!isset($this->known['reports'][$name])) {
            $reportclass = @$this->config()->reports[$name];

            if (!$reportclass) {
                throw new Exception("No such report '{$name}'");
            }

            $report = new $reportclass();
            $report->name = $name;

            $report->jars($this);
            $report->filesystem($this->filesystem());

            if (method_exists($report, 'init')) {
                $report->init($token);
            }

            $this->known['reports'][$name] = $report;
        }

        return $this->known['reports'][$name];
    }

    public function takeanumber()
    {
        $pointer_file = $this->db_home . '/pointer.dat';
        $id = $this->n2h($pointer = $this->filesystem->get($pointer_file) ?? 1);
        $this->filesystem->put($pointer_file, $pointer + 1);

        return $id;
    }

    public function filesystem()
    {
        if (func_num_args()) {
            $filesystem = func_get_arg(0);

            if (!($filesystem instanceof Filesystem)) {
                throw new Exception(__METHOD__ . ': argument should be instance of Filesystem');
            }

            $prev = $this->filesystem;
            $this->filesystem = $filesystem;

            return $prev;
        }

        return $this->filesystem;
    }

    public function token()
    {
        if (func_num_args()) {
            $token = func_get_arg(0);

            if (!is_string($token)) {
                throw new Exception(__METHOD__ . ': argument should be a string');
            }

            $prev = $this->token;
            $this->token = $token;

            return $prev;
        }

        return $this->token;
    }

    public function db_path($path = null)
    {
        return $this->db_home . ($path ? '/' . $path : null);
    }

    public static function of(string $portal_home, string $db_home)
    {
        return new Jars($portal_home, $db_home);
    }

    public function get_childset(string $linetype, string $id, string $property)
    {
        return $this->linetype($linetype)->get_childset($this->token, $id, $property);
    }
}
