<?php

namespace jars;

use jars\contract\BadTokenException;
use jars\contract\BadUsernameOrPasswordException;
use jars\contract\ConcurrentModificationException;
use jars\contract\Config;
use jars\contract\ConfigException;
use jars\contract\Constants;
use jars\contract\Exception;

class Jars implements contract\Client
{
    private $touch_handle = null;
    private ?Filesystem $filesystem;
    private static ?object $debug_node = null;
    private static ?object $debug_root = null;
    private ?string $head = null;
    private ?string $locker_pin = null;
    private ?string $token = null;
    private array $known = [];
    private array $listeners = [];
    private array $verified_tokens = [];
    private Config $config;
    private string $db_home;

    const INITIAL_VERSION = 'd6711496d746ac3688c7de4ed14988c6ac0af8244b42a6c48298d9fff331c701';

    public function __construct(Config $config, string $db_home)
    {
        $this->config = $config;
        $this->db_home = $db_home;
        $this->filesystem = new Filesystem();

        if (!($this->config->linetypes()['token'] ?? null)) {
            throw new Exception('Missing token linetype');
        }
    }

    public function __clone()
    {
        $this->filesystem = clone $this->filesystem;
    }

    private static function array_keys_recursive(array $array, string $separtor = '/'): array
    {
        $keys = [];

        foreach ($array as $key => $element) {
            if (is_array($element)) {
                $keys = array_merge($keys, array_map(fn ($subkey) => $key . $separtor . $subkey, static::array_keys_recursive($element, $separtor)));
            } else {
                $keys[] = $key;
            }
        }

        return $keys;
    }

    public function children(string $linetype_name): array
    {
        if (!$this->verify_token($this->token())) {
            throw new BadTokenException;
        }

        return $this->linetype($linetype_name)->childInfo();
    }

    private static function classifier_value($classify, $line): array
    {
        if (is_callable($classify)) {
            $classify = ($classify)($line);
        }

        if (is_null($classify)) {
            return [];
        }

        if (is_string($classify)) {
            $classify = [$classify];
        }

        if (!is_array($classify)) {
            throw new Exception('Could not resolve classification result to an array');
        }

        if ($wrong = array_filter($classify, fn ($group) => !is_string($group) || !preg_match('/^' . Constants::GROUP_PATTERN . '$/', $group))) {
            throw new Exception('Invalid classification results [' . implode(',', array_map(fn ($w) => json_encode($w, JSON_UNESCAPED_SLASHES), $wrong)) . ']');
        }

        return array_values($classify); // be forgiving of non-numeric or non-sequential indices
    }

    private function commit(string $timestamp, array $commits, array $meta, array $saves, ?string $base_version): void
    {
        foreach ($commits as $id => $commit) {
            if (!count(array_diff(array_keys(get_object_vars($commit)), ['id', 'type']))) {
                unset($commits[$id]);
            }
        }

        if (!count($commits)) {
            return;
        }

        $add_meta = array_filter($meta, fn ($change) => substr($change, 0, 1) === '+');
        $nonadd_meta = array_filter($meta, fn ($change) => substr($change, 0, 1) !== '+');

        $modified_ids = array_diff($this->meta_ids($nonadd_meta), $this->meta_ids($add_meta));

        @mkdir($version_dir = $this->db_home . '/versions', 0777, true);

        if ($modified_ids && !$base_version) {
            throw new ConcurrentModificationException("Incorrect base version. Head: [$this->head], base version: none");
        }

        $master_meta_file = $this->masterlog_meta_file();

        $this->head = $this->db_version();

        $version_number = $this->version_number_of($this->head);

        // complain if this would cause concurrent modification

        if ($modified_ids && $base_version !== $this->head) {
            $from = $this->version_number_of($base_version) + 1;
            $comparison_metas = explode("\n", trim(`cat '$master_meta_file' | tail -n +$from | cut -c66- | sed 's/ /\\n/g'` ?? ''));

            $comodified_ids = array_map(
                fn ($change) => preg_replace('/.*:/', '', $change),
                array_filter($comparison_metas, fn ($change) => substr($change, 0, 1) !== '+'),
            );

            if (array_intersect($comodified_ids, $modified_ids)) {
                throw new ConcurrentModificationException("Incorrect base version. Head: [$this->head], base version: [$base_version]");
            }
        }

        $master_export = $timestamp . ' ' . json_encode(array_values($commits), JSON_UNESCAPED_SLASHES);
        $meta_export = implode(' ', $meta);

        $this->head = hash('sha256', $this->head . $master_export);

        $this->db_version($this->head);
        $this->filesystem->put($version_dir . '/' . $this->head, $version_number + 1);
        $this->filesystem->append($master_meta_file, $this->head . ' ' . $meta_export . "\n");
        $this->filesystem->append($this->masterlog_file(), $this->head . ' ' . $master_export . "\n");

        foreach ($saves as $save) {
            if (!$save->record->oldrecord) {
                $save->record->created_version = $this->head;
            }

            $save->record->modified_version = $this->head;

            $save->record->save();
        }
    }

    public function config()
    {
        return $this->config;
    }

    public function db_path(?string $path = null): string
    {
        return $this->db_home . ($path ? '/' . $path : null);
    }

    private function db_version(?string $new = null): string|self
    {
        $db_version_file = $this->db_path('version.dat');

        if (func_num_args()) {
            $this->filesystem->put($db_version_file, $new, 200);

            return $this;
        }

        return $this->filesystem->get($db_version_file) ?? static::INITIAL_VERSION;
    }

    public static function debug_push(string $activity): void
    {
        if (!defined('JARS_CORE_DEBUG') || !JARS_CORE_DEBUG) {
            return;
        }

        $node = (object) [
            'start' => microtime(true),
            'activity' => $activity,
            'parent' => static::$debug_node,
            'children' => [],
        ];

        if (null === static::$debug_node) {
            static::$debug_root = static::$debug_node = $node;
        } else {
            static::$debug_node->children[] = $node;
            static::$debug_node = $node;
        }
    }

    public static function debug_pop(): void
    {
        if (!defined('JARS_CORE_DEBUG') || !JARS_CORE_DEBUG) {
            return;
        }

        if (null === static::$debug_node) {
            error_log('Debug failure: tried to pop but nothing to left pop');
        }

        // close off this node first

        static::$debug_node->end = microtime(true);
        static::$debug_node->duration = static::$debug_node->end - static::$debug_node->start;

        for (
            $level = 0, $_node = static::$debug_node;
            $_node !== null;
            $level++, $_node = $_node->parent
        );

        // display what we know so far in error log

        error_log(str_repeat(' ', 4 * $level) . static::$debug_node->activity . ' finished in ' . number_format(static::$debug_node->duration, 8) . 's');

        // now pop back to the parent

        static::$debug_node = static::$debug_node->parent;
    }

    public function delete(string $linetype_name, string $id): array
    {
        if (!$this->verify_token($this->token())) {
            throw new BadTokenException;
        }

        $this->head = $this->db_version();

        return $this->linetype($linetype_name)->delete($id);
    }

    private function dredge_r(array $lines): array
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

    public function fields(string $linetype_name): array
    {
        if (!$this->verify_token($this->token())) {
            throw new BadTokenException;
        }

        return $this->linetype($linetype_name)->fieldInfo();
    }

    public function filesystem(?Filesystem $filesystem = null): null|Filesystem|self
    {
        if (func_num_args()) {
            $this->filesystem = $filesystem;

            return $this;
        }

        return $this->filesystem;
    }

    public function find_table_linetypes(string $table_name): array
    {
        if (!$this->verify_token($this->token())) {
            throw new BadTokenException;
        }

        $found = [];

        foreach (array_keys($this->config->linetypes()) as $linetype_name) {
            $linetype = $this->linetype($linetype_name);

            if ($linetype->table === $table_name) {
                $found[] = $linetype;
            }
        }

        return $found;
    }

    public function flatten(?object $object = null): ?object
    {
        $return = null;

        foreach (func_get_args() as $_object) {
            if (!is_object($_object)) {
                throw new Exception('All arguments should be objects');
            }

            $return ??= $_object;

            foreach (array_keys(get_object_vars($_object)) as $key) {
                if (!is_null($_object->$key) && !is_scalar($_object->$key)) {
                    unset($_object->$key);
                }
            }
        }

        return $return;
    }

    public function get(string $linetype_name, string $id): ?object
    {
        if (!$this->verify_token($this->token())) {
            throw new BadTokenException;
        }

        $this->head = $this->db_version();

        $line = $this->linetype($linetype_name)->get($this->token, $id);

        if (!$line) {
            return null;
        }

        return $line;
    }

    public function get_childset(string $linetype_name, string $id, string $property): array
    {
        if (!$this->verify_token($this->token())) {
            throw new BadTokenException;
        }

        return $this->linetype($linetype_name)->get_childset($this->token, $id, $property, $lines_cache);
    }

    public function group(string $report_name, string $group = '', string|bool|null $min_version = null)
    {
        if (!$this->verify_token($this->token())) {
            throw new BadTokenException;
        }

        $report = $this->report($report_name);
        $group = $report->get($group, $min_version === true ? $this->head : ($min_version ?: null));

        $this->head = $report->version();

        return $group;
    }

    public function groups(string $report_name, string $prefix = '', string|bool|null $min_version = null): array
    {
        if (!$this->verify_token($this->token())) {
            throw new BadTokenException;
        }

        $report = $this->report($report_name);
        $groups = $report->groups($prefix, $min_version === true ? $this->head : ($min_version ?: null));

        $this->head = $report->version();

        return $groups;
    }

    public function h2n(string $h, ?int $max = null): ?int
    {
        if (!$this->verify_token($this->token())) {
            throw new BadTokenException;
        }

        $sequence = $this->config->sequence();

        for ($n = 1; $n <= ($max ?? INF); $n++) {
            if ($this->n2h($n) == $h) {
                return $n;
            }
        }

        return null;
    }

    public function import(string $timestamp, array $lines, ?string $base_version = null, bool $dryrun = false, ?int $logging = null, bool $differential = false): array
    {
        $base_version ??= $this->head;

        if (!$this->verify_token($this->token())) {
            throw new BadTokenException;
        }

        if ($i_lock = !$dryrun && !isset($this->locker_pin)) {
            $this->lock();
        }

        // we have the floor!

        try {
            return $this->_import($timestamp, $lines, $base_version, $dryrun, $logging, $differential);
        } finally {
            if ($i_lock) {
                $this->unlock_internal();
            }
        }
    }

    private function _import(string $timestamp, array $lines, ?string $base_version, bool $dryrun, ?int $logging, bool $differential): array
    {
        $affecteds = [];
        $commits = [];
        $original_filesystem = clone $this->filesystem;
        $original_filesystem->freeze();

        static::debug_push('Jars::import_r');
        $lines = $this->import_r($original_filesystem, $timestamp, $lines, $base_version, $affecteds, $commits, null, $logging, $differential);
        static::debug_pop();

        static::debug_push('Process affecteds');
        foreach ($affecteds as $affected) {
            switch ($affected->action) {
                case 'connect':
                    Link::of($this, $affected->tablelink, $affected->left)
                        ->add($affected->right)
                        ->save();

                    Link::of($this, $affected->tablelink, $affected->right, true)
                        ->add($affected->left)
                        ->save();

                    $meta[] = '>' . $affected->tablelink . ':' . $affected->left . ',' . $affected->right;

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

                    $meta[] = '<' . $affected->tablelink . ':' . $affected->left . ',' . $affected->right;

                    break;

                case 'save':
                    $affected->record->save();
                    $meta[] = ($affected->oldrecord ? '~' : '+') . $affected->table . ':' . $affected->id;

                    break;

                default:
                    throw new Exception('Unknown action: ' . @$affected->action);
            }
        }
        static::debug_pop();

        static::debug_push('Dredge');
        $updated = $this->dredge_r($lines);
        static::debug_pop();

        static::debug_push('Commit activities');
        if ($dryrun) {
            $this->filesystem->reset();
        } else {
            $commits = array_filter($commits);
            $saves = array_filter($affecteds, fn ($affected) => $affected->action === 'save');

            $this->commit($timestamp, $commits, $meta, $saves, $base_version);
        }
        static::debug_pop();

        static::debug_push('Trigger entryimported');
        $this->trigger('entryimported');
        static::debug_pop();

        return $updated;
    }

    public function import_r(Filesystem $original_filesystem, string $timestamp, array $lines, ?string $base_version, array &$affecteds, array &$commits, ?string $ignorelink = null, ?int $logging = null, bool $differential = false): array
    {
        if (!$this->verify_token($this->token())) {
            throw new BadTokenException;
        }

        foreach ($lines as $line) {
            if (!is_object($line)) {
                throw new Exception('Lines should be an array of objects');
            } elseif (!property_exists($line, 'type')) {
                throw new Exception('All lines must have a type');
            } elseif (!array_key_exists($line->type, $this->config->linetypes())) {
                throw new Exception('Unrecognised linetype: ' . $line->type);
            }
        }

        foreach ($lines as $line) {
            $this
                ->linetype($line->type)
                ->import($this->token, $original_filesystem, $timestamp, $line, $base_version, $affecteds, $commits, $ignorelink, $logging, $differential);
        }

        foreach ($lines as $line) {
            $this
                ->linetype($line->type)
                ->recurse_to_children($original_filesystem, $timestamp, $line, $base_version, $affecteds, $commits, $ignorelink, $logging, $differential);
        }

        return $lines;
    }

    public function info(?string $varname = null): array|string|null
    {
        $info = [
            'config_class' => get_class($this->config),
            'db_home' => $this->db_home,
            'touch_file' => $this->touch_file(),
        ];

        $info['connection_string'] = "local:$info[config_class],$info[db_home]";

        if ($varname !== null) {
            return $info[strtolower($varname)] ?? null;
        }

        return $info;
    }

    public function linetype(string $name): object
    {
        if (!isset($this->known['linetypes'][$name])) {
            $linetypeclass = $this->config->linetypes()[$name] ?? null;

            if (!$linetypeclass) {
                throw new Exception("No such linetype '{$name}'");
            }

            $linetype = new $linetypeclass();
            $linetype->name = $name;

            $linetype->jars($this);

            if (method_exists($linetype, 'init')) {
                $linetype->init($token);
            }

            $this->known['linetypes'][$name] = $linetype;
        }

        return $this->known['linetypes'][$name];
    }

    public function linetypes(?string $report_name = null): array
    {
        if (!$this->verify_token($this->token())) {
            throw new BadTokenException;
        }

        if ($report_name) {
            if (!array_key_exists($report_name, $this->config->reports())) {
                throw new Exception('No such report [' . $report_name . ']');
            }

            $names = [];

            foreach ($this->report($report_name)->listen as $key => $value) {
                $name = is_string($value) ? $value : $key;

                if (!preg_match('/^report:/', $name)) {
                    $names[] = $name;
                }
            }
        } else {
            $names = array_keys($this->config->linetypes());
        }

        sort($names);

        $linetypes = array_map(fn ($name) => (object) [
            'name' => $name,
            'fields' => $this->fields($name),
            'children' => $this->children($name),
        ], $names);

        return $linetypes;
    }

    public function listen(Listener $listener): void
    {
        $this->listeners[] = $listener;
    }

    private function load_children_r(object $line, array $children, array &$childsets, ?array &$lines_cache = null): void
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
                $line_childsets[$property] = $this->get_childset($line->type, $line->id, $property, $lines_cache);
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
                    $this->load_children_r($childline, $child->children, $childsets, $lines_cache);
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

    public function lock(): false|string
    {
        if ($this->touch_handle) {
            throw new Exception('Attempt to lock when already locked');
        }

        @mkdir($this->db_home, 0777, true);

        // TODO: Implement timeout using non-blocking locking in a loop until

        if (!($this->touch_handle = fopen($this->touch_file(), 'a'))) {
            throw new Exception('Could not open the touch file');
        }

        if (!flock($this->touch_handle, LOCK_EX)) {
            fclose($this->touch_handle);

            $this->touch_handle = null;

            throw new Exception('Could not acquire a lock over the touch file');
        }

        return $this->locker_pin = bin2hex(random_bytes(32));
    }

    public function login(?string $username = null, ?string $password = null, bool $one_time = false): ?string
    {
        $start = microtime(true) * 1e6;

        try {
            if (!$this->config->credentialsCorrect($username, $password)) {
                throw new BadUsernameOrPasswordException('Unknown username or incorrect password');
            }
        } catch (\Exception $e) {
            $end = $start + (float) 1e5; // make sure failure always takes 0.1s
            usleep(max(0, $end - microtime(true) * 1e6));

            throw $e;
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

    public function logout(): bool
    {
        if (!$line = $this->verify_token($this->token)) {
            return true;
        }

        $lines = $this->save([(object)[
            'id' => $line->id,
            'type' => 'token',
            '_is' => false,
        ]]);

        return property_exists($token = reset($lines), '_is') && $token->_is === false;
    }

    public function masterlog_check(): void
    {
        if (!$this->verify_token($this->token())) {
            throw new BadTokenException;
        }

        if (!static::writable($this->masterlog_file())) {
            throw new Exception('Master record file not writable');
        }

        if (!static::writable($this->masterlog_meta_file())) {
            throw new Exception('Master meta file not writable');
        }
    }

    public function masterlog_file(): string
    {
        return $this->db_path('master.dat');
    }

    public function masterlog_meta_file(): string
    {
        return $this->db_path('master.dat.meta');
    }

    private function meta_ids(array $meta): array
    {
        return array_unique(array_merge(...array_map(fn ($change) => explode(
            ',',
            preg_replace('/.*:/', '', $change),
        ), $meta)));
    }

    public function n2h(int $n): string
    {
        if (!$this->verify_token($this->token())) {
            throw new BadTokenException;
        }

        $sequence = $this->config->sequence();

        return hash('sha256', hex2bin(hash('sha256', $n . '--' . $sequence->secret())));
    }

    public static function of(Config $config, string $db_home): self
    {
        return new Jars($config, $db_home);
    }

    public function persist(): self
    {
        $this->filesystem->persist();

        return $this;
    }

    public function preview(array $lines, ?string $base_version = null): array
    {
        if (!$this->verify_token($this->token())) {
            throw new BadTokenException;
        }

        if (!$lines) {
            return $lines;
        }

        return $this->import(date('Y-m-d H:i:s'), $lines, $base_version, true);
    }

    private function propagate_r(string $table_name, string $id, array &$relatives, array &$changes = [], array &$seen = []): void
    {
        foreach ($this->find_table_linetypes($table_name) as $linetype) {
            $relationships = array_merge(
                $linetype->find_incoming_links(),
                $linetype->find_incoming_inlines(),
            );

            foreach ($relationships as $relationship) {
                $direction = ($relationship->reverse ?? false) ? 'forth' : 'back';
                $lost_relatives = $relatives[$relationship->tablelink][$direction][$id] ?? [];
                $current_relatives = Link::of($this, $relationship->tablelink, $id, !@$relationship->reverse)->relatives();
                $_relatives = array_unique(array_merge($lost_relatives, $current_relatives));

                foreach ($_relatives as $relative_id) {
                    $relative_linetype = $this->linetype($relationship->parent_linetype);
                    $table_name = $relative_linetype->table;

                    if (!Record::of($this, $table_name, $relative_id)->exists()) {
                        continue;
                    }

                    $change = (object) [
                        'table' => $table_name,
                        'sign' => '*',
                    ];

                    if (!isset($changes[$relative_id])) {
                        $changes[$relative_id] = $change;
                    }

                    if (!isset($seen[$key = $table_name . ':' . $relative_id])) {
                        $seen[$key] = true;

                        $this->propagate_r($table_name, $relative_id, $relatives, $changes, $seen);
                    }
                }
            }
        }
    }

    public function record(string $table_name, string $id, ?string &$content_type = null, ?string &$filename = null): ?string
    {
        if (!$this->verify_token($this->token())) {
            throw new BadTokenException;
        }

        $tableinfo = $this->config->tables()[$table_name] ?? null;
        $ext = $tableinfo->extension ?? 'json';
        $filename = $id . ($ext ? '.' . $ext : null);
        $content_type = $tableinfo->content_type ?? 'application/json';

        if (!is_file($file = $this->db_path('records/' . $table_name . '/' . $filename))) {
            return null;
        }

        $this->head = $this->db_version();

        return file_get_contents($file);
    }

    public function refresh(): string
    {
        if (!$this->verify_token($this->token())) {
            throw new BadTokenException;
        }

        $lines_dir = $this->db_path('reports/.refreshd/lines');

        if (!is_dir($lines_dir)) {
            mkdir($lines_dir, 0777, true);
        }

        if ($i_lock = !isset($this->locker_pin)) {
            $this->lock();
        }

        // we have the floor!

        try {
            $bunny = $this->db_version();
            $bunny_number = (int) $this->filesystem->get($this->db_path('versions/' . $bunny));
            $changed_reports = [];

            // preload the metas we need

            $from = INF;

            foreach (array_keys($this->config->reports()) as $report_name) {
                $report = $this->report($report_name);

                if ($report->is_fully_derived()) {
                    continue;
                }

                $greyhound = $report->version($greyhound_number) ?? static::INITIAL_VERSION;;

                if ($greyhound_number < $from) {
                    $from = $greyhound_number;
                    $from_greyhound = $greyhound;
                }
            }

            $length = $bunny_number - $from;

            if (!$length) {
                // nothing to do

                return $bunny;
            }

            if (defined('JARS_VERBOSE') && JARS_VERBOSE) {
                error_log("Refreshing $from_greyhound ($from) ~ $bunny ($bunny_number) (Δ $length)...");
            }

            $master_meta_file = $this->db_path('master.dat.meta');
            $command = "cat '$master_meta_file' | tail -n +" . ($from + 1) . " | head -n $length | cut -c66-";
            $process = proc_open($command, [1 => ['pipe', 'w']], $pipes);

            if (!is_resource($process)) {
                throw new Exception('Problem reading master log meta file');
            }

            $metas = array_map(fn () => explode(' ', rtrim(fgets($pipes[1]), "\n")), array_fill($from, $length, null));

            fclose($pipes[1]);
            proc_close($process);

            foreach (array_keys($this->config->reports()) as $report_name) {
                $report = $this->report($report_name);

                if ($report->is_fully_derived()) {
                    continue;
                }

                $greyhound = $report->version($greyhound_number);

                if ($greyhound && $bunny == $greyhound) {
                    continue;
                }

                if (defined('JARS_VERBOSE') && JARS_VERBOSE) {
                    error_log("Refreshing report $report_name [$greyhound_number → $bunny_number]");
                }

                $changes = [];
                $relatives = [];

                for ($version_number = $greyhound_number; $version_number <= $bunny_number - 1; $version_number++) {
                    foreach ($metas[$version_number] as $meta) {
                        // connection

                        if (preg_match('/^>/', $meta)) {
                            continue;
                        }

                        // disconnection - keep track of linked record ids

                        if (preg_match('/^<([^:,]+):([^:,]+),([^:,]+)$/', $meta, $matches)) {
                            [, $tablelink, $left, $right] = $matches;

                            $relatives[$tablelink]['forth'][$left][] = $right;
                            $relatives[$tablelink]['back'][$right][] = $left;
                            continue;
                        }

                        if (!preg_match('/^([+-~])([a-z_]+):([a-zA-Z0-9+\/=]+)$/', $meta, $matches)) {
                            throw new Exception('Invalid meta line: ' . $meta);
                        }

                        [, $sign, $table, $id] = $matches;

                        if (!isset($changes[$id])) {
                            $changes[$id] = (object) [
                                'table' => $table,
                            ];
                        }

                        $changes[$id]->sign = $sign;
                    }
                }

                $lines_cache = [];
                $childsets = [];

                // propagate

                if ($greyhound) {
                    foreach ($changes as $id => $change) {
                        $this->propagate_r($change->table, $id, $relatives, $changes);
                    }
                }

                $report_affected = false;

                foreach ($changes as $id => $change) {
                    foreach ($report->listen as $linetype => $listen) {
                        if (is_numeric($linetype)) {
                            $linetype = $listen;
                            $listen = (object) [];
                        }

                        if (preg_match('/^report:/', $linetype)) {
                            continue;
                        }

                        if ($change->table != $this->linetype($linetype)->table) {
                            continue;
                        }

                        $report_affected = true;

                        $current_groups = [];
                        $past_groups = [];

                        if (in_array($change->sign, ['+', '~', '*'])) {
                            if (!isset($lines_cache[$linetype])) {
                                $lines_cache[$linetype] = [];
                            }

                            if (!isset($lines_cache[$linetype][$id])) {
                                $lines_cache[$linetype][$id] = $this->get($linetype, $id);
                            }

                            $line = clone $lines_cache[$linetype][$id];

                            $this->load_children_r($line, @$listen->children ?? [], $childsets, $lines_cache);

                            try {
                                if (property_exists($listen, 'classify') && $listen->classify) {
                                    $current_groups = static::classifier_value($listen->classify, $line);
                                } elseif (property_exists($report, 'classify') && $report->classify) {
                                    $current_groups = static::classifier_value($report->classify, $line);
                                } else {
                                    $current_groups = [''];
                                }
                            } catch (Exception $e) {
                                throw new Exception($e->getMessage() . ' in report [' . $report_name . ']');
                            }
                        }

                        $groups_file = $lines_dir . '/' . $report_name . '/' . $linetype . '/' . $id . '.json';

                        if (!is_dir($parent = dirname($groups_file))) {
                            if (!is_dir($grandparent = dirname($groups_file, 2))) {
                                mkdir($grandparent);
                            }

                            mkdir($parent);
                        }

                        if (in_array($change->sign, ['-', '~', '*'])) {
                            $past_groups = $this->filesystem->has($groups_file) ? json_decode($this->filesystem->get($groups_file))->groups : [];
                        }

                        // remove

                        foreach (array_diff($past_groups, $current_groups) as $group) {
                            $report->delete($group, $linetype, $id);
                        }

                        // upsert

                        foreach ($current_groups as $group) {
                            $report->upsert($group, $line, @$report->sorter);
                        }

                        if ($current_groups) {
                            $this->filesystem->put($groups_file, json_encode(['groups' => $current_groups], JSON_UNESCAPED_SLASHES));
                        } elseif ($this->filesystem->has($groups_file)) {
                            $this->filesystem->delete($groups_file);
                        }

                        foreach (array_merge($past_groups, $current_groups) as $group) {
                            $changed_reports[$report_name][$group] = true;
                        }
                    }
                }

                if ($report_affected) {
                    $this->filesystem->put($report->version_file(), $bunny, 200); // the greyhound has caught the bunny!
                    $this->filesystem->persist()->reset();
                }
            }

            $this->head = $bunny;

            $this->refresh_derived(static::array_keys_recursive($changed_reports), $bunny);
        } finally {
            if ($i_lock) {
                $this->unlock_internal(false);
            }
        }

        return $bunny;
    }

    public function refresh_derived(array $changed, string $version, array &$cache = []): void
    {
        if (!$this->verify_token($this->token())) {
            throw new BadTokenException;
        }

        $new_changed = [];

        foreach (array_keys($this->config->reports()) as $derived_reportname) {
            $derived_report = $this->report($derived_reportname);

            $report_affected = false;

            foreach ($changed as $change_report_group) {
                [$change_reportname, $change_groupname] = explode('/', $change_report_group, 2);

                foreach ($derived_report->listen as $base_report => $listen) {
                    if (is_numeric($base_report)) {
                        $base_report = $listen;
                        $listen = (object) [];
                    }

                    if (!preg_match('/^report:(.*)/', $base_report, $groups)) {
                        continue;
                    }

                    $base_report = $groups[1];

                    if ($base_report !== $change_reportname) {
                        continue;
                    }

                    $report_affected = true;

                    try {
                        if (property_exists($listen, 'classify') && $listen->classify) {
                            $derived_groupnames = @static::classifier_value($listen->classify, $change_groupname);
                        } elseif (property_exists($derived_report, 'classify') && $derived_report->classify) {
                            $derived_groupnames = @static::classifier_value($derived_report->classify, $change_groupname);
                        } else {
                            $derived_groupnames = [''];
                        }
                    } catch (Exception $e) {
                        throw new Exception($e->getMessage() . ' in report [' . $report_name . ']');
                    }

                    foreach ($derived_groupnames as $derived_groupname) {
                        $cache[$change_reportname][$change_groupname] ??= $this->group($change_reportname, $change_groupname, $version);
                        $cache[$derived_reportname][$derived_groupname] ??= $this->group($derived_reportname, $derived_groupname);

                        if (defined('JARS_VERBOSE') && JARS_VERBOSE) {
                            error_log("Refreshing derived report $derived_reportname from $change_reportname/$change_groupname to derived group $derived_groupname");
                        }

                        $cache[$derived_reportname][$derived_groupname] = $derived_report->handle(
                            $cache[$change_reportname][$change_groupname],
                            $cache[$derived_reportname][$derived_groupname],
                            $change_reportname,
                            $change_groupname,
                            $derived_groupname,
                        );

                        $new_changed[$derived_reportname . '/' . $derived_groupname] = true;

                        $group_file = $this->db_path("reports/$derived_reportname/$derived_groupname.json");

                        if ($exists = $cache[$derived_reportname][$derived_groupname] !== null) {
                            $this->filesystem->put($group_file, json_encode($cache[$derived_reportname][$derived_groupname], JSON_UNESCAPED_SLASHES));

                            if (!in_array($derived_groupname, $groups)) {
                                $groups[] = $derived_groupname;

                                sort($groups);
                            }
                        } else {
                            $this->filesystem->delete($group_file);

                            if (in_array($derived_groupname, $groups)) {
                                $groups = array_values(array_diff($groups, [$derived_groupname]));
                            }
                        }

                        $derived_report->maintain_groups($derived_groupname, $exists);
                    }
                }
            }

            if ($report_affected) {
                $this->filesystem->put($derived_report->version_file(), $version, 200); // the greyhound has caught the bunny!
                $this->filesystem->persist()->reset();
            }
        }

        if ($new_changed) {
            $this->refresh_derived(array_keys($new_changed), $version, $cache);
        }
    }

    public function report(string $name): object
    {
        if (!$this->verify_token($this->token())) {
            throw new BadTokenException;
        }

        if (!isset($this->known['reports'][$name])) {
            $reportclass = $this->config->reports()[$name] ?? null;

            if (!$reportclass) {
                throw new Exception("No such report '{$name}'");
            }

            $report = new $reportclass();
            $report->name = $name;

            $report->jars($this);
            $report->filesystem($this->filesystem());

            if (method_exists($report, 'init')) {
                $report->init();
            }

            $this->known['reports'][$name] = $report;
        }

        return $this->known['reports'][$name];
    }

    public function reports(): array
    {
        if (!$this->verify_token($this->token())) {
            throw new BadTokenException;
        }

        $reports = [];

        foreach (array_keys($this->config->reports()) as $name) {
            $report = $this->report($name);
            $fields = [];

            foreach ($this->config->report_fields()[$name] ?? ['id'] as $field) {
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
                'is_derived' => $report->is_derived(),
            ];
        }

        usort($reports, fn ($a, $b) => $a->name <=> $b->name);

        return $reports;
    }

    public function save(array $lines, ?string $base_version = null): array
    {
        if (!$this->verify_token($this->token())) {
            throw new BadTokenException;
        }

        if (!$lines) {
            return $lines;
        }

        static::debug_push('Jars::import');

        $result = $this->import(date('Y-m-d H:i:s'), $lines, $base_version);

        static::debug_pop();

        return $result;
    }

    public function takeanumber(): string
    {
        if (!$this->verify_token($this->token())) {
            throw new BadTokenException;
        }

        $pointer_file = $this->db_home . '/pointer.dat';
        $id = $this->n2h($pointer = $this->filesystem->get($pointer_file) ?? 1);

        $this->trigger('takeanumber', $pointer, $id);
        $this->filesystem->put($pointer_file, $pointer + 1);

        return $id;
    }

    public function token(?string $token = null): self|string|null
    {
        if (func_num_args()) {
            $this->token = $token;

            return $this;
        }

        return $this->token;
    }

    public function touch(): object
    {
        if (!$this->verify_token($this->token())) {
            throw new BadTokenException;
        }

        $this->head = $this->db_version();

        return (object) [
            'timestamp' => time(),
        ];
    }

    private function touch_file(): string
    {
        return $this->db_home . '/touch.dat';
    }

    public function trigger(string $event, ...$arguments): void
    {
        if (!preg_match('/[a-z]+/', $event)) {
            throw new Exception('Invalid event name');
        }

        $eventinterface = 'jars\\events\\' . $event;

        if (!interface_exists($eventinterface)) {
            throw new Exception('No such event [' . $event . ']');
        }

        foreach ($this->listeners as $listener) {
            if (is_subclass_of($listener, $eventinterface)) {
                $method = 'handle_' . $event;

                $listener->$method(...$arguments);
            }
        }
    }

    public function unlock(string $locker_pin, bool $do_touch = true): void
    {
        if (!$this->touch_handle) {
            throw new Exception('Attempt to unlock when not locked');
        }

        if ($this->locker_pin !== $locker_pin) {
            throw new Exception('Incorrect locker PIN provided for unlocking');
        }

        $this->filesystem->persist()->reset();

        if ($do_touch) {
            ftruncate($this->touch_handle, 0);
            fwrite($this->touch_handle, microtime(true));
        }

        fclose($this->touch_handle);

        $this->touch_handle = null;
        $this->locker_pin = null;
    }

    private function unlock_internal(bool $touch = true): void
    {
        $this->unlock($this->locker_pin, $touch);
    }

    public static function validate_password(string $password): bool
    {
        return
            is_string($password)
            && strlen($password) > 5;
    }

    public static function validate_username(string $username): bool
    {
        return
            is_string($username)
            && strlen($username) > 0
            && (
                preg_match('/^[a-z0-9_]+$/', $username)
                || filter_var($username, FILTER_VALIDATE_EMAIL) !== false
            );
    }

    public function verify_token(string $token): object|false
    {
        if (isset($this->verified_tokens[$token])) {
            return $this->verified_tokens[$token];
        }

        if (!preg_match('/^([a-zA-Z0-9]+):([0-9a-f]{64})$/', $token, $groups)) {
            return false;
        }

        // token deleted or never existed

        [, $id, $random_bits] = $groups;

        $time = microtime(true);

        $line = null;

        try {
            $line = $this->linetype('token')->get(null, $id);
        } catch (\Exception $e) {}

        if (
            !$line
            || $line->token !== $random_bits
            || $line->ttl + $line->used < time()
        ) {
            usleep(floor((0.5 + $time - microtime(true)) * 1000000)); // don't let on whether the line existed

            return false;
        }

        return $this->verified_tokens[$token] = (object) [
            'id' => $line->id,
            'token' => $line->token,
        ];
    }

    public function version(): ?string
    {
        if (!$this->verify_token($this->token())) {
            throw new BadTokenException;
        }

        return $this->head;
    }

    private function version_number_of(string $version): int
    {
        if ($version === static::INITIAL_VERSION) {
            return 0;
        }

        $file = $this->db_path('versions/' . $version);

        if (null === $number = $this->filesystem->get($file)) {
            throw new Exception('Could not resolve version [' . $version . '] to a number');
        }

        return intval($number);
    }

    private static function writable(string $file): bool
    {
        return touch($file) && is_writable($file);
    }
}
