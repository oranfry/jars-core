<?php

namespace OranFry\Jars\Core;

use OranFry\Jars\Contract\BadTokenException;
use OranFry\Jars\Contract\BadUsernameOrPasswordException;
use OranFry\Jars\Contract\ConcurrentModificationException;
use OranFry\Jars\Contract\Config;
use OranFry\Jars\Contract\ConfigException;
use OranFry\Jars\Contract\Constants;
use OranFry\Jars\Contract\Exception;

class Jars implements \OranFry\Jars\Contract\Client
{
    private Filesystem $filesystem;
    private Locker $locker;
    private static ?object $debug_node = null;
    private static ?object $debug_root = null;
    private ?int $head = null;
    private ?int $pointer = null;
    private ?string $token = null;
    private array $known = [];
    private array $listeners = [];
    private int $numIssued = 0;
    private array $verified_tokens = [];
    private Config $config;
    private string $db_home;

    private array $recordStore = [];
    private array $linkStore = [];
    private array $proposedRecordStore = [];
    private array $proposedLinkStore = [];

    public function __clone()
    {
        $this->filesystem = clone $this->filesystem;
    }

    public function __construct(Config $config, string $db_home)
    {
        $this->config = $config;
        $this->db_home = $db_home;
        $this->filesystem = new Filesystem($this->db_path('tmp'));
        $this->locker = new Locker($this->touch_file(), $this->reports_lock_file());

        if (!($this->config->linetypes()['token'] ?? null)) {
            throw new Exception('Missing token linetype');
        }
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

    public static function classify(object $line, object $listen, Report $report): array
    {
        if (property_exists($listen, 'classify') && $listen->classify) {
            return static::classifier_value($listen->classify, $line);
        }

        if (property_exists($report, 'classify') && $report->classify) {
            return static::classifier_value($report->classify, $line);
        }

        return [''];
    }

    private function commit(string $timestamp, array $commits, array $meta, ?int $base_version): void
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

        if ($modified_ids && !$base_version) {
            throw new ConcurrentModificationException("Incorrect base version. Head: [$this->head], base version: none");
        }

        // complain if this would cause concurrent modification

        if ($modified_ids && $base_version !== $this->head) {
            $comparison_metas = [];

            for ($_version = $base_version; $_version < $this->head; $_version++) {
                $comparison_metas = array_merge($comparison_metas, $this->getMeta($_version + 1));
            }

            $comodified_ids = array_map(
                fn ($change) => preg_replace('/.*:/', '', $change),
                array_filter($comparison_metas, fn ($change) => substr($change, 0, 1) !== '+'),
            );

            if (array_intersect($comodified_ids, $modified_ids)) {
                throw new ConcurrentModificationException("Incorrect base version. Head: [$this->head], base version: [$base_version]");
            }
        }

        $this->filesystem->put($this->versionInfoFile($this->head + 1), (object) [
            'pointer' => $this->pointer + $this->numIssued,
            'meta' => $meta,
        ]);

        $this->filesystem->put(
            $this->masterFile($this->head + 1),
            $timestamp . ' ' . json_encode(array_values($commits), JSON_UNESCAPED_SLASHES) . "\n",
            true,
        );
    }

    public function config()
    {
        return $this->config;
    }

    private function _dataFile($prefix, string $id, ...$types): string
    {
        if (!preg_match('/^[a-f0-9]{64}$/', $id)) {
            throw new Exception('Invalid ID');
        }

        $numSubParts = defined('JARS_CORE_DATAFILE_NUM_SUBPARTS') ? (int) JARS_CORE_DATAFILE_NUM_SUBPARTS : 2;
        $subPartLength = defined('JARS_CORE_DATAFILE_SUBPART_LENGTH') ? (int) JARS_CORE_DATAFILE_SUBPART_LENGTH : 2;

        if ($subPartLength * $numSubParts > 64) {
            throw new Exception('Invalid subpart config: subdir would be too long');
        }

        if ($subPartLength < 1 || $numSubParts < 1) {
            throw new Exception('Invalid subpart config: subdir would be too short');
        }

        $subParts = [];

        for ($p = 0; $p < $numSubParts; $p++) {
            $subParts[] = substr($id, $subPartLength * $p, $subPartLength);
        }

        $subdir = implode('/', $subParts) . '/';
        $suffix = implode('.', array_filter($types));

        if ($suffix) {
            $suffix = '.' . $suffix;
        }

        return $this->db_path($prefix . $subdir . $id . $suffix);
    }

    public function dataFile(string $id, ...$types): string
    {
        return $this->_dataFile('data/', $id, ...$types);
    }

    public function db_path(?string $path = null): string
    {
        return $this->db_home . ($path ? '/' . $path : null);
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

        $this->loadVersionInfo();

        return $this->linetype($linetype_name)->delete($id);
    }

    private function dredge_r(array $lines, int $version): array
    {
        $dredged = [];

        foreach ($lines as $line) {
            if (@$line->_is !== false) {
                $_line = $this->linetype($line->type)->get($this->token, $line->id, $version);
            } else {
                $_line = (object) [
                    '_is' => false,
                    'id' => $line->id,
                    'type' => $line->type,
                ];
            }

            foreach (array_keys(get_object_vars($line)) as $key) {
                if (is_array($line->$key)) {
                    $_line->$key = $this->dredge_r($line->$key, $version);
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

        $this->loadVersionInfo();

        $line = $this->linetype($linetype_name)->get($this->token, $id, $this->head);

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

        return $this->linetype($linetype_name)->get_childset($this->token, $id, $this->head, $property, $lines_cache);
    }

    private function getMeta(int $version): array
    {
        return $this->getVersionInfo($version)->meta;
    }

    private function getPointer(int $version): int
    {
        return $this->getVersionInfo($version)->pointer;
    }

    private function getVersionInfo(int $version): object
    {
        if (!$info = $this->filesystem->get($this->versionInfoFile($version))) {
            throw new Exception("No version info for $version");
        }

        return $info;
    }

    public function group(string $report_name, string $group = '', int|true|null $min_version = null)
    {
        if (!$this->verify_token($this->token())) {
            throw new BadTokenException;
        }

        $report = $this->report($report_name);
        $group = $report->get($group, $min_version === true ? $this->head : $min_version);

        $this->head = $report->version();

        return $group;
    }

    public function groups(string $report_name, string $prefix = '', int|true|null $min_version = null): array
    {
        if (!$this->verify_token($this->token())) {
            throw new BadTokenException;
        }

        $report = $this->report($report_name);
        $groups = $report->groups($prefix, $min_version === true ? $this->head : $min_version);

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

    public function import(
        string $timestamp,
        array $lines,
        ?int $base_version = null,
        bool $dryrun = false,
        ?int $logging = null,
        bool $differential = false,
    ): array
    {
        if (!$this->verify_token($this->token())) {
            throw new BadTokenException;
        }

        if ($iLock = !$dryrun && !$this->locker->isPrimaryLocked()) {
            $pin = $this->lockPrimary();
            $this->head = $this->locker->head();
        }

        // we have the floor!

        $base_version ??= $this->head;

        $this->loadVersionInfo();

        try {
            $result = $this->_import($timestamp, $lines, $base_version, $dryrun, $logging, $differential);

            if ($iLock) {
                $this->unlockPrimary($pin, $this->head);
            }

            return $result;
        } catch (\Exception $e) {
            if ($iLock) {
                $this->unlockPrimary($pin);
            }

            throw $e;
        }
    }

    private function _import(string $timestamp, array $lines, ?int $base_version, bool $dryrun, ?int $logging, bool $differential): array
    {
        $affecteds = [];
        $commits = [];

        static::debug_push('Jars::import_r');

        $lines = $this->import_r(
            $timestamp,
            $lines,
            $base_version,
            $affecteds,
            $commits,
            null,
            $logging,
            $differential,
        );

        static::debug_pop();

        static::debug_push('Process affecteds');

        foreach ($affecteds as $affected) {
            switch ($affected->action) {
                case 'connect':
                    Link::propose($this, $affected->tablelink, $this->head + 1, $affected->left)
                        ->add($affected->right)
                        ->save();

                    Link::propose($this, $affected->tablelink, $this->head + 1, $affected->right, true)
                        ->add($affected->left)
                        ->save();

                    $meta[] = '>' . $affected->tablelink . ':' . $affected->left . ',' . $affected->right;

                    break;

                case 'delete':
                    $affected->record = Record::propose($this, $affected->table, $this->head + 1, $affected->oldrecord->id);
                    $affected->record->delete()->save();

                    $meta[] = '-' . $affected->table . ':' . $affected->id;

                    break;

                case 'disconnect':
                    Link::propose($this, $affected->tablelink, $this->head + 1, $affected->left)
                        ->remove($affected->right)
                        ->save();

                    Link::propose($this, $affected->tablelink, $this->head + 1, $affected->right, true)
                        ->remove($affected->left)
                        ->save();

                    $meta[] = '<' . $affected->tablelink . ':' . $affected->left . ',' . $affected->right;

                    break;

                case 'save':
                    if (!$affected->oldrecord) {
                        $affected->record->created = $timestamp;
                        $affected->record->created_version = $this->head + 1;
                    }

                    $affected->record->modified = $timestamp;
                    $affected->record->modified_version = $this->head + 1;

                    $affected->record->save();

                    $meta[] = ($affected->oldrecord ? '~' : '+') . $affected->table . ':' . $affected->id;

                    break;

                default:
                    throw new Exception('Unknown action: ' . @$affected->action);
            }
        }

        static::debug_pop();

        if ($dryrun) {
            static::debug_push('Dredge');

            $updated = $this->dredge_r($lines, $this->head + 1);

            static::debug_pop();

            $this->filesystem->reset();
            $this->numIssued = 0;

            return $updated;
        }

        static::debug_push('Commit activities');

        $this->commit($timestamp, array_filter($commits), $meta, $base_version);

        static::debug_pop();

        static::debug_push('Trigger entryimported');

        $this->trigger('entryimported');

        static::debug_pop();

        static::debug_push('Dredge');

        $updated = $this->dredge_r($lines, $this->head + 1);

        static::debug_pop();

        $this->head = $this->head + 1;
        $this->pointer = $this->pointer + $this->numIssued;
        $this->numIssued = 0;


        return $updated;
    }

    public function import_r(
        string $timestamp,
        array $lines,
        ?int $base_version,
        array &$affecteds,
        array &$commits,
        ?string $ignorelink = null,
        ?int $logging = null,
        bool $differential = false,
    ): array
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
                ->import(
                    $this->token,
                    $timestamp,
                    $line,
                    $this->head,
                    $base_version,
                    $affecteds,
                    $commits,
                    $ignorelink,
                    $logging,
                    $differential,
                );
        }

        foreach ($lines as $line) {
            $this
                ->linetype($line->type)
                ->recurse_to_children(
                    $timestamp,
                    $line,
                    $base_version,
                    $affecteds,
                    $commits,
                    $ignorelink,
                    $logging,
                    $differential,
                );
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

    public function isPrimaryLocked(): bool
    {
        return $this->locker->isPrimaryLocked();
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

    public function linkStore(string $key, ?Link $link = null): ?Link
    {
        if (func_num_args() > 1) {
            $this->linkStore[$key] = $link;
        }

        return $this->linkStore[$key] ?? null;
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

    private function loadVersionInfo(): void
    {
        if ($this->head === null) {
            $this->head = $this->filesystem->get($this->touch_file()) ?? 0;
        }

        $this->pointer = $this->head === 0 ? 1 : $this->getPointer($this->head);
    }

    public function lockPrimary(): ?string
    {
        return $this->locker->lockPrimary();
    }

    public function lockReports(): ?string
    {
        return $this->locker->lockReports();
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

    private function masterFile(int $version): string
    {
        return $this->db_path(implode('/', [
            'master',
            sprintf("%04x", ($version >> 16)),
            sprintf("%02x", ($version >> 8) & 255),
            sprintf("%02x", $version),
        ]));
    }

    private function meta_ids(array $meta): array
    {
        return array_unique(array_merge(...array_map(fn ($change) => explode(
            ',',
            preg_replace('/.*:/', '', $change),
        ), $meta)));
    }

    private function versionInfoFile(int $version): string
    {
        return $this->dataFile(
            hash('sha256', 'version-' . $version),
            'version',
        );
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

    public function preview(array $lines, ?int $base_version = null): array
    {
        if (!$this->verify_token($this->token())) {
            throw new BadTokenException;
        }

        if (!$lines) {
            return $lines;
        }

        return $this->import(date('Y-m-d H:i:s'), $lines, $base_version, true);
    }

    private function propagate_r(string $table_name, string $id, int $version, array &$relatives, array &$changes = [], array &$seen = []): void
    {
        foreach ($this->find_table_linetypes($table_name) as $linetype) {
            $relationships = array_merge(
                $linetype->find_incoming_links(),
                $linetype->find_incoming_inlines(),
            );

            foreach ($relationships as $relationship) {
                $direction = ($relationship->reverse ?? false) ? 'forth' : 'back';
                $lost_relatives = $relatives[$relationship->tablelink][$direction][$id] ?? [];
                $current_relatives = Link::of($this, $relationship->tablelink, $version, $id, !@$relationship->reverse)->relatives();
                $_relatives = array_unique(array_merge($lost_relatives, $current_relatives));

                foreach ($_relatives as $relative_id) {
                    $relative_linetype = $this->linetype($relationship->parent_linetype);
                    $table_name = $relative_linetype->table;

                    if (!Record::of($this, $table_name, $version, $relative_id)->exists()) {
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

                        $this->propagate_r($table_name, $relative_id, $version, $relatives, $changes, $seen);
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

        $this->loadVersionInfo();

        return Record::of($this, $table_name, $this->head, $id)
            ->currentContents();
    }

    public function recordStore(string $key, ?Record $record = null): ?Record
    {
        if (func_num_args() > 1) {
            $this->recordStore[$key] = $record;
        }

        return $this->recordStore[$key] ?? null;
    }

    public function refresh(): string
    {
        if (!$this->verify_token($this->token())) {
            throw new BadTokenException;
        }

        $pin = $this->lockReports();

        // we have the floor!

        try {
            $this->loadVersionInfo();

            $bunny = $this->head;
            $changed_reports = [];

            // preload the metas we need

            $all_reports = array_keys($this->config->reports());
            $unaffected_reports = array_flip($all_reports);

            $change_clumps = [];

            foreach ($all_reports as $report_name) {
                $report = $this->report($report_name);

                if ($report->is_fully_derived()) {
                    continue;
                }

                $greyhound = $report->version();

                if ($greyhound && $bunny === $greyhound) {
                    continue;
                }

                if (defined('JARS_VERBOSE') && JARS_VERBOSE) {
                    error_log("Refreshing report $report_name [$greyhound → $bunny]");
                }

                $clump_key = $greyhound . '~' . $bunny;

                if (!isset($change_clumps[$clump_key])) {
                    $changes = [];
                    $relatives = [];

                    for ($version = $greyhound; $version <= $bunny - 1; $version++) {
                        $forRemoval = [];

                        foreach ($this->getMeta($version + 1) as $meta) {
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

                            if (@$changes[$id]->sign === '+' && $sign === '-') {
                                // added and deleted; line has no effect
                                $forRemoval[$id] = true;
                            } elseif (!isset($changes[$id]->sign) || $changes[$id]->sign !== '+' || $sign !== '~') {
                                // update sign, except plus to updated
                                $changes[$id]->sign = $sign;
                            }
                        }

                        foreach (array_keys($forRemoval) as $id) {
                            unset($changes[$id]);
                        }
                    }

                    // propagate

                    if ($greyhound) {
                        foreach ($changes as $id => $change) {
                            $this->propagate_r($change->table, $id, $bunny, $relatives, $changes);
                        }
                    }

                    $change_clumps[$clump_key] = $changes;
                }

                $change = $change_clumps[$clump_key];

                $lines_cache = [];
                $childsets = [];

                $report_affected = false;

                if (defined('JARS_VERBOSE') && JARS_VERBOSE) {
                    $numChanges = count($changes);
                    $numChangesStrLen = strlen($numChanges);
                    error_log("    Found [$numChanges] changes");
                }

                $count = 0;

                if (defined('JARS_VERBOSE') && JARS_VERBOSE) {
                    $starttime = microtime(true);
                }

                foreach ($changes as $id => $change) {
                    if (defined('JARS_VERBOSE') && JARS_VERBOSE && $count && ($count === $numChanges || !($count % 1000))) {
                        $now = microtime(true);
                        $delta = number_format($now - $starttime, 4) . ' s';
                        $percent = number_format(($count / $numChanges) * 100, 2);
                        error_log("    $delta Processing change [ " . str_pad($count, $numChangesStrLen, ' ', STR_PAD_LEFT) . " / $numChanges ] " . str_pad($percent, 5, ' ', STR_PAD_LEFT) . "%");
                    }

                    $count++;

                    foreach ($report->listen as $linetype_name => $listen) {
                        if (is_numeric($linetype_name)) {
                            $linetype_name = $listen;
                            $listen = (object) [];
                        }

                        if (preg_match('/^report:/', $linetype_name)) {
                            continue;
                        }

                        if ($change->table != $this->linetype($linetype_name)->table) {
                            continue;
                        }

                        $report_affected = true;
                        $current_groups = [];

                        if (in_array($change->sign, ['+', '~', '*'])) {
                            if (!isset($lines_cache[$linetype_name])) {
                                $lines_cache[$linetype_name] = [];
                            }

                            if (!isset($lines_cache[$linetype_name][$id])) {
                                $lines_cache[$linetype_name][$id] = $this->get($linetype_name, $id);
                            }

                            $line = clone $lines_cache[$linetype_name][$id];

                            $this->load_children_r($line, @$listen->children ?? [], $childsets, $lines_cache);

                            try {
                                $current_groups = static::classify($line, $listen, $report);
                            } catch (Exception $e) {
                                throw new Exception($e->getMessage() . ' in report [' . $report_name . ']');
                            }
                        }

                        $past_groups = [];

                        if (in_array($change->sign, ['-', '~', '*'])) {
                            $oldline = $this->linetype($linetype_name)->get($this->token, $id, $greyhound);
                            $past_groups = static::classify($oldline, $listen, $report);
                        }

                        // remove

                        foreach (array_diff($past_groups, $current_groups) as $group) {
                            $report->delete($group, $linetype_name, $id);
                        }

                        // upsert

                        foreach ($current_groups as $group) {
                            $report->upsert($group, $line, @$report->sorter);
                        }

                        foreach (array_merge($past_groups, $current_groups) as $group) {
                            $changed_reports[$report_name][$group] = true;
                        }
                    }
                }

                if ($report_affected) {
                    $this->filesystem->put($report->version_file(), $bunny, false, 200); // the greyhound has caught the bunny!

                    unset($unaffected_reports[$report_name]);
                }

                if (defined('JARS_VERBOSE') && JARS_VERBOSE) {
                    error_log("    Refreshed report $report_name [$greyhound → $bunny]");
                }
            }

            $this->head = $bunny;

            $this->refresh_derived(static::array_keys_recursive($changed_reports), $bunny, $unaffected_reports);

            // bump latest version of unchanged reports too

            if ($unaffected_reports) {
                foreach (array_keys($unaffected_reports) as $report_name) {
                    $this->filesystem->put($this->report($report_name)->version_file(), $bunny, false, 200);
                }
            }

            $this->filesystem->persist()->reset();
        } finally {
            if (isset($pin)) {
                $this->unlockReports($pin);
            }
        }

        return $bunny;
    }

    public function refresh_derived(array $changed, int $version, array &$unaffected_reports, array &$cache = []): void
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

                    if (defined('JARS_VERBOSE') && JARS_VERBOSE) {
                        error_log("Refreshing derived report $derived_reportname");
                    }

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
                            $this->filesystem->put($group_file, $cache[$derived_reportname][$derived_groupname]);

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
                $this->filesystem->put($derived_report->version_file(), $version, false, 200); // the greyhound has caught the bunny!

                unset($unaffected_reports[$derived_reportname]);
            }
        }

        if ($new_changed) {
            $this->refresh_derived(array_keys($new_changed), $version, $unaffected_reports, $cache);
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

    private function reports_lock_file(): string
    {
        return $this->db_home . '/reports/lock';
    }

    public function save(array $lines, ?int $base_version = null): array
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

        static::debug_push('Jars::persist');

        $this->persist();

        static::debug_pop();

        return $result;
    }

    public function takeANumber(): string
    {
        if (!$this->verify_token($this->token())) {
            throw new BadTokenException;
        }

        $idNumber = $this->pointer + $this->numIssued;
        $id = $this->n2h($idNumber);

        $this->trigger('takeanumber', $idNumber, $id);

        $this->numIssued++;

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

        $this->loadVersionInfo();

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

        $eventinterface = 'OranFry\\Jars\\Core\\Events\\' . $event;

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

    public function unlockPrimary(string $pin): self
    {
        $this->filesystem->persist();
        $this->locker->unlockPrimary($pin, $this->head);

        return $this;
    }

    public function unlockReports(string $pin): self
    {
        $this->filesystem->persist();
        $this->locker->unlockReports($pin);

        return $this;
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

        $this->loadVersionInfo();

        try {
            $line = $this->linetype('token')->get(null, $id, $this->head);
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

    public function version(): ?int
    {
        if (!$this->verify_token($this->token())) {
            throw new BadTokenException;
        }

        return $this->head;
    }

    private static function writable(string $file): bool
    {
        return touch($file) && is_writable($file);
    }
}
