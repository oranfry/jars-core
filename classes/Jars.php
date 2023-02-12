<?php

namespace jars;

use Exception;

class Jars implements contract\Client
{
    private $db_home;
    private $filesystem;
    private $head;
    private $known = [];
    private $listeners = [];
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

        $line = null;

        try {
            $line = $this->linetype('token', true)->get(null, $id);
        } catch (Exception $e) {}

        if (
            !$line
            ||
            $line->token !== $random_bits
            ||
            $line->ttl + $line->used < time()
        ) {
            usleep(floor((0.5 + $time - microtime(true)) * 1000000)); // don't let on whether the line existed

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

        if (!$dryrun && file_exists($current_version_file = $this->db_home . '/version.dat') && !file_exists("{$this->db_home}/past")) {
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

        $updated = $this->dredge_r($lines);

        if ($dryrun) {
            $this->filesystem->revert();
        } else {
            $commits = array_filter($commits);

            $this->commit($timestamp, $commits, $meta);

            if (defined('JARS_PERSIST_PER_IMPORT') && JARS_PERSIST_PER_IMPORT) {
                $this->filesystem->persist();
            }
        }

        $this->trigger('entryimported');

        return $updated;
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
        // Generate a sequence secret: php -r 'echo base64_encode(random_bytes(33)) . "\n";'
        $sequence = $this->config()->sequence;

        $banned = array_unique(array_merge(@$sequence->banned_chars ?? [], ['+', '/', '=']));
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

        $id = substr(str_replace($banned, '', base64_encode(hex2bin(hash('sha256', $n . '--' . $sequence_secret)))), 0, $sequence->size ?? 12);

        if (isset($sequence->transform)) {
            $id = call_user_func($sequence->transform, $id);
        }

        return $id;
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
        return $this->linetype($linetype)->fieldInfo();
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
            return false;
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

        $this->trigger('takeanumber', $pointer, $id);
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

    public function refresh() : string
    {
        $reports_dir = $this->db_path('reports');
        $lines_dir = $reports_dir . '/.refreshd/lines';

        if (!is_dir($lines_dir)) {
            mkdir($lines_dir, 0777, true);
        }

        $bunny = $this->filesystem->get($this->db_path('/version.dat'));
        $bunny_number = (int) $this->filesystem->get($this->db_path('versions/' . $bunny));

        $greyhound = null;
        $greyhound_number = 0;

        if ($this->filesystem->has($greyhound_file = $reports_dir . "/version.dat")) {
            $greyhound = $this->filesystem->get($greyhound_file);
            $greyhound_number = (int) $this->filesystem->get($this->db_path('versions/' . $greyhound));
        }

        if ($bunny == $greyhound) {
            return $greyhound;
        }

        if ($bunny_number > $greyhound_number) {
            $from = $greyhound_number + 1;
            $direction = 'forward';
            $sorter = null;
        } else {
            $from = $bunny_number + 1;
            $direction = 'back';
            $sorter = 'array_reverse';
        }

        $length = abs($bunny_number - $greyhound_number);
        $master_meta_file = $this->db_path('master.dat.meta');
        $metas = explode("\n", trim(`cat '$master_meta_file' | tail -n +$from | head -n $length | cut -c66- | sed 's/ /\\n/g'`));

        if ($sorter) {
            $metas = $sorter($metas);
        }

        $changes = [];

        foreach ($metas as $meta) {
            if (!preg_match('/^([+-~])([a-z]+):([a-zA-Z0-9+\/=]+)$/', $meta, $matches)) {
                throw new Exception('Invalid meta line: ' . $meta);
            }

            list(, $sign, $type, $id) = $matches;

            if (!isset($changes[$id])) {
                $changes[$id] = (object) [
                    'type' => $type,
                ];
            }

            $changes[$id]->sign = $sign;
        }

        $lines = [];
        $childsets = [];

        // propagate

        if ($greyhound) {
            foreach ($changes as $id => $change) {
                $this->propagate_r($change->type, $id, $greyhound, $changes);
            }
        }

        foreach (array_keys($this->config()->reports) as $report_name) {
            $report = $this->report($report_name);

            foreach ($changes as $id => $change) {
                foreach ($report->listen as $linetype => $listen) {
                    if (is_numeric($linetype)) {
                        $linetype = $listen;
                        $listen = (object) [];
                    }

                    $table = $this->linetype($linetype)->table;

                    if ($change->type != $table) {
                        continue;
                    }

                    $current_groups = [];
                    $past_groups = [];

                    if (in_array($change->sign, ['+', '~', '*'])) {
                        if (!isset($lines[$linetype])) {
                            $lines[$linetype] = [];
                        }

                        $linetype_lines = &$lines[$linetype];

                        if (!isset($linetype_lines[$id])) {
                            $linetype_lines[$id] = $this->get($linetype, $id);
                        }

                        $line = clone $lines[$linetype][$id];

                        $this->load_children_r($line, @$listen->children ?? [], $childsets);

                        if (property_exists($listen, 'classify') && $listen->classify) {
                            $current_groups = static::classifier_value($listen->classify, $line);
                        } elseif (property_exists($report, 'classify') && $report->classify) {
                            $current_groups = static::classifier_value($report->classify, $line);
                        } else {
                            $current_groups = ['all'];
                        }
                    }

                    $groups_file = $lines_dir . '/' . $report_name . '/' . $id . '.json';

                    if (!is_dir(dirname($groups_file))) {
                        mkdir(dirname($groups_file));
                    }

                    if (in_array($change->sign, ['-', '~', '*'])) {
                        $past_groups = $this->filesystem->has($groups_file) ? json_decode($this->filesystem->get($groups_file))->groups : [];
                    }

                    // remove

                    if (!is_array($current_groups)) {
                        throw new Exception($current_groups);
                    }

                    foreach (array_diff($past_groups, $current_groups) as $group) {
                        $report->delete($group, $id);
                    }

                    // upsert

                    foreach ($current_groups as $group) {
                        $report->upsert($group, $line, @$report->sorter);
                    }

                    if ($current_groups) {
                        $this->filesystem->put($groups_file, json_encode(['groups' => $current_groups]));
                    } elseif ($this->filesystem->has($groups_file)) {
                        $this->filesystem->delete($groups_file);
                    }
                }
            }
        }

        $past_dir = $this->db_path('past');

        // TODO: This assumes we never interact with past dir via Filesystem class, so implement Filesystem::find() instead

        foreach (array_filter(explode("\n", `test -d "$past_dir" && find "$past_dir" -type f || true` ?? '')) as $pastfile) {
            $this->filesystem->put($pastfile, null);
        };

        $this->filesystem->put($greyhound_file, $bunny); // the greyhound has caught the bunny!

        return $bunny;
    }

    private static function classifier_value($classify, $line) {
        if (is_array($classify)) {
            return $classify;
        }

        if (is_string($classify)) {
            return [$classify];
        }

        if (is_callable($classify)) {
            $groups = ($classify)($line);

            if (!is_array($groups) || array_filter($groups, function ($group) { return !is_string($group) || !$group; })) {
                throw new Exception('Invalid classication result');
            }

            return $groups;
        }

        throw new Exception('Invalid classifier');
    }

    private function load_children_r(object $line, array $children, array &$childsets)
    {
        foreach ($children as $property => $child) {
            if (is_numeric($property)) {
                $property = $child;
                $child = (object) [];
            }

            if (!isset($childsets[$line->type])) {
                $childsets[$line->type] = [];
            }

            $linetype_childsets = &$childsets[$line->type];

            if (!isset($linetype_childsets[$line->id])) {
                $linetype_childsets[$line->id] = [];
            }

            $line_childsets = &$linetype_childsets[$line->id];

            if (!isset($line_childsets[$property])) {
                $line_childsets[$property] = $this->get_childset($line->type, $line->id, $property);
            }

            $childset = $line_childsets[$property];

            if (property_exists($child, 'filter')) {
                if (!is_callable($child->filter)) {
                    throw new Exception('Invalid filter');
                }

                $childset = array_filter($childset, $child->filter);
            }

            if (property_exists($child, 'children')) {
                if (!is_array($child->children)) {
                    throw new Exception('Invalid children');
                }

                foreach ($childset as $childline) {
                    $this->load_children_r($childline, $child->children, $childsets);
                }
            }

            if (property_exists($child, 'latefilter')) {
                if (!is_callable($child->latefilter)) {
                    throw new Exception('Invalid filter');
                }

                $childset = array_filter($childset, $child->latefilter);
            }

            if (property_exists($child, 'sorter')) {
                if (!is_callable($child->sorter)) {
                    throw new Exception('Invalid sorter');
                }

                usort($childset, $child->sorter);
            }

            $line->$property = array_values($childset);
        }
    }

    private function propagate_r(string $linetype, string $id, string $version, array &$changes = [], array &$seen = [])
    {
        $linetype = $this->linetype($linetype);

        $relatives = array_merge(
            $linetype->find_incoming_links(),
            $linetype->find_incoming_inlines(),
        );

        foreach ($relatives as $relative) {
            $links = [
                Link::of($this, $relative->tablelink, $id, !@$relative->reverse, $version),
                Link::of($this, $relative->tablelink, $id, !@$relative->reverse)
            ];

            foreach ($links as $link) {
                foreach ($link->relatives() as $relative_id) {
                    $relative_linetype = $this->linetype($relative->parent_linetype);
                    $table = $relative_linetype->table;

                    if (!Record::of($this, $table, $relative_id)->exists()) {
                        continue;
                    }

                    $change = (object) [
                        'type' => $table,
                        'sign' => '*',
                    ];

                    if (!isset($changes[$relative_id])) {
                        $changes[$relative_id] = $change;
                    }

                    if (!isset($seen[$key = $relative->parent_linetype . ':' . $relative_id])) {
                        $seen[$key] = true;

                        $this->propagate_r($relative->parent_linetype, $relative_id, $version, $changes, $seen);
                    }
                }
            }
        }
    }

    public function reports() : array
    {
        $reports = [];
        $config = $this->config();

        foreach (array_keys($config->reports) as $name) {
            $report = $this->report($name);
            $fields = [];

            foreach (@$config->report_fields[$name] ?? ['id'] as $field) {
                if (is_string($field)) {
                    $field = (object) ['name' => $field];
                }

                if (!@$field->type) {
                    $field->type = 'string';
                }

                $fields[] = $field;
            }

            $reports[] = (object) [
                'name' => $name,
                'fields' => $fields,
            ];
        }

        usort($reports, fn ($a, $b) => $a->name <=> $b->name);

        return $reports;
    }

    public function linetypes(?string $report = null) : array
    {
        if ($report) {
            if (!array_key_exists($report, $this->config()->reports)) {
                throw new Exception('No such report [' . $report . ']');
            }

            $names = [];

            foreach ($this->report($report)->listen as $key => $value) {
                if (is_string($value)) {
                    $names[] = $value;
                } else {
                    $names[] = $key;
                }
            }
        } else {
            $names = array_keys($this->config()->linetypes);
        }

        sort($names);

        $linetypes = array_map(fn ($name) => (object) [
            'name' => $name,
            'fields' => $this->fields($name),
        ], $names);

        return $linetypes;
    }

    private function dredge_r($lines)
    {
        $dredged = [];

        foreach ($lines as $line) {
            if (@$line->_is !== false) {
                $_line = $this->linetype($line->type)->get($this->token, $line->id);
            } else {
                $_line = (object) [
                    '_is' => false,
                    'id' => $line->id,
                    'type' => $line->type,
                ];
            }

            foreach (array_keys(get_object_vars($line)) as $key) {
                if (is_array($line->$key)) {
                    $_line->$key = $this->dredge_r($line->$key);
                }
            }

            $dredged[] = $_line;
        }

        return $dredged;
    }

    public function listen(Listener $listener): void
    {
        $this->listeners[] = $listener;
    }

    public function trigger(string $event, ...$arguments): void
    {
        if (!preg_match('/[a-z]+/', $event)) {
            error_response('invalid event name');
        }

        $eventinterface = 'jars\\events\\' . $event;

        if (!interface_exists($eventinterface)) {
            error_response('no such event [' . $event . ']');
        }

        foreach ($this->listeners as $listener) {
            if (is_subclass_of($listener, $eventinterface)) {
                $method = 'handle_' . $event;

                $listener->$method(...$arguments);
            }
        }
    }
}
