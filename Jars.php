<?php

namespace OranFry\Jars\Core;

use Closure;
use OranFry\Jars\Contract\BadTokenException;
use OranFry\Jars\Contract\BadUsernameOrPasswordException;
use OranFry\Jars\Contract\ConcurrentModificationException;
use OranFry\Jars\Contract\Config;
use OranFry\Jars\Contract\ConfigException;
use OranFry\Jars\Contract\Constants;
use OranFry\Jars\Contract\Exception;

class Jars implements \OranFry\Jars\Contract\Client
{
    const ROOT_VERSION = '00000096d746ac3688c7de4ed14988c6ac0af8244b42a6c48298d9fff331c701';

    const ENCODING_OPTIONS = JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES;

    private static ?object $debug_node = null;
    private static ?object $debug_root = null;

    private ?string $head = null;
    private ?string $token = null;
    private ?string $version = null;
    private array $known = [];
    private array $listeners = [];
    private array $verified_tokens = [];
    private Config $config;
    private int $pointer = 1;

    public Master $master;
    public Chain $chain;
    public Index $index;
    public Reporter $reporter;

    public function __construct(Config $config, string $chainHome, string $indexHome, string $reportsHome, string $masterHome)
    {
        if (!($config->linetypes()['token'] ?? null)) {
            throw new Exception('Missing token linetype');
        }

        // var_dump($config->tables()); die();

        $this->config = $config;

        $this->index = new Index($this, $indexHome);
        $this->chain = new Chain($this, $chainHome);
        $this->reporter = new Reporter($this, $reportsHome);
        $this->master = new Master($this, $masterHome);
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

    public function config()
    {
        return $this->config;
    }

    public function dataFile($base_path, string $id, ...$suffixes): string
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

        $subdir = implode('/', $subParts);
        $suffix = implode('.', array_filter($suffixes));

        if ($suffix) {
            $suffix = '.' . $suffix;
        }

        return $base_path . '/' . $subdir . '/' . $id . $suffix;
    }

    public function db_path(?string $path = null): string
    {
        echo "db_path called\n";

        foreach (debug_backtrace() as $item) {
            echo @$item['file'] . ':' . @$item['line'] . "\n";
        }

        die("\n\n");
    }

    private function db_version(): string
    {
        $file = $this->db_path('current/version.dat');

        return is_file($file) ? file_get_contents($file) : static::ROOT_VERSION;
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

        // $this->head = $this->db_version();

        return $this->linetype($linetype_name)->delete($id);
    }

    private function dredge_r(array $lines, string $version): array
    {
        $dredged = [];

        foreach ($lines as $line) {
            $_line = match ($line->_is ?? true) {
                false => (object) [
                    '_is' => false,
                    'id' => $line->id,
                    'type' => $line->type,
                ],
                default => $this->linetype($line->type)->get($this->token, $line->id),
            };

            $_line->version = $version;

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
        throw new Exception('filesystem used');
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

        // $this->head = $this->index->head();

        $line = $this
            ->linetype($linetype_name)
            ->get($this->token, $id);

        if (!$line) {
            return null;
        }

        $line->version = $this->index->head();

        return $line;
    }

    public function get_childset(string $linetype_name, string $id, string $property): array
    {
        if (!$this->verify_token($this->token())) {
            throw new BadTokenException;
        }

        return $this->linetype($linetype_name)->get_childset($this->token, $id, $property, $lines_cache);
    }

    public function group(string $report_name, string $group = '', string|bool|null $min_version = null, ?int $timeout = null)
    {
        if (!$this->verify_token($this->token())) {
            throw new BadTokenException;
        }

        $manager = $this->reporter->getManager($report_name);

        return $manager->get(
            new Filesystem(),
            $group,
            $min_version === true ? $this->head : ($min_version ?: null),
            $timeout ?? ReportManager::DEFAULT_TIMEOUT,
        );
    }

    public function groups(string $report_name, string $prefix = '', string|bool|null $min_version = null, ?int $timeout = null): array
    {
        if (!$this->verify_token($this->token())) {
            throw new BadTokenException;
        }

        $manager = $this->reporter->getManager($report_name);

        return $manager->groups(
            new Filesystem(),
            $prefix,
            $min_version === true ? $this->head : ($min_version ?: null),
            $timeout ?? ReportManager::DEFAULT_TIMEOUT,
        );
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
        ?string $baseVersion = null,
        bool $dryrun = false,
        ?int $logging = null,
        bool $differential = false,
        ?string $version = null,
    ): array
    {
        $headBlock = $this->headBlock();

        if (!$dryrun) {
            try {
                $headBlock->lock();
            } catch (\OranFry\Jars\Contract\AlreadyLockedException $e) {
                throw new ConcurrentModificationException($e->getMessage());
            }
        }

        $success = false;

        try {
            $baseBlock = $this
                ->chain
                ->getBlock($baseVersion ?? static::ROOT_VERSION);

            $comodified = [];

            while ($baseBlock && $baseBlock !== $headBlock) {
                foreach ($this->meta_ids($baseBlock->meta()) as $comodifiedId) {
                    $comodified[$comodifiedId] = true;
                }

                $baseBlock = $baseBlock->advance();
            }

            if (!$baseBlock) {
                throw new Exception('Unknown base block or could not advance to head from given base block');
            }

            // baseBlock is now identical to headBlock, this is just a sanity check for certainty

            if ($baseBlock !== $headBlock) {
                throw new Exception('Unexpected condition, baseBlock is not identical to headBlock');
            }

            $comodified = array_keys($comodified);

            $commits = [];

            $newBlock = $this
                ->chain
                ->createBlock($baseBlock, $timestamp, $version);

            static::debug_push('Jars::import_r');

            $lines = $this->import_r(
                $timestamp,
                $lines,
                $baseBlock,
                $newBlock,
                $commits,
                null,
                $logging,
                $differential,
            );

            static::debug_pop();

            if (!count($commits = array_filter($commits))) {
                return [];
            }

            static::debug_push('Check for comodification');

            $add_meta = array_filter($newBlock->meta(), fn ($change) => substr($change, 0, 1) === '+');
            $nonadd_meta = array_filter($newBlock->meta(), fn ($change) => substr($change, 0, 1) !== '+');
            $modified = array_diff($this->meta_ids($nonadd_meta), $this->meta_ids($add_meta));

            // complain if this would cause concurrent modification

            if ($conflict = array_intersect($comodified, $modified)) {
                throw new ConcurrentModificationException(
                    "Record modification conflict: " .
                    $baseBlock->version() .
                    ' vs ' .
                    $newBlock->version() .
                    ':' .
                    implode(', ', $conflict),
                );
            }

            static::debug_pop();

            if (!$dryrun) {
                $newBlock->save();

                if (Jars::ROOT_VERSION === $baseBlock->version()) {
                    $baseBlock->mkdir();
                }

                static::debug_push('Link to previous block');

                $baseBlock->setNext($newBlock->version());
                $success = true;

                static::debug_pop();
            }
        } finally {
            if (!$dryrun) {
                $headBlock->unlock();

                if (!$success) {
                    unlink($headBlock->lockFile());
                }
            }
        }

        static::debug_push('Update index');
        if (!$dryrun) {
            $this->index->safeLock(10);
        }

        $this->index->addToChain($newBlock, !$dryrun);

        if (!$dryrun) {
            $this->index->unlock();
        }
        static::debug_pop();

        if (!$dryrun) {
            static::debug_push('Write master');
            $this->master->write(
                $baseBlock->version(),
                $newBlock->version(),
                $timestamp,
                json_encode(array_values($commits), JSON_UNESCAPED_SLASHES),
            );
            static::debug_pop();
        }

        static::debug_push('Dredge');
        $updated = $this->dredge_r($lines, $newBlock->version());
        static::debug_pop();

        if ($dryrun) {
           $this->index->revert();
        }

        static::debug_push('Trigger entryimported');
        $this->trigger('entryimported');
        static::debug_pop();

        return $updated;
    }

    public function connect(string $tablelink, string $left, string $right, Block $newBlock): self
    {
        $newBlock->addLink($tablelink, $left, false, $right);
        $newBlock->addLink($tablelink, $right, true, $left);

        $newBlock->addMeta('>' . $tablelink . ':' . $left . ',' . $right);

        return $this;
    }

    public function disconnect(string $tablelink, string $left, string $right, Block $newBlock): self
    {
        $newBlock->removeLink($tablelink, $left, false, $right);
        $newBlock->removeLink($tablelink, $right, true, $left);

        $newBlock->addMeta('<' . $tablelink . ':' . $left . ',' . $right);

        return $this;
    }

    public function import_r(
        string $timestamp,
        array $lines,
        Block $baseBlock,
        Block $newBlock,
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
                    $baseBlock,
                    $newBlock,
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
                    $baseBlock,
                    $newBlock,
                    $commits,
                    $ignorelink,
                    $logging,
                    $differential,
                );

            $line->version = $newBlock->version();
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

    public static function of(Config $config, string $chainHome, string $indexHome, string $reportsHome, string $masterHome): self
    {
        return new Jars($config, $chainHome, $indexHome, $reportsHome, $masterHome);
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
                $current_relatives = $this->getLink($relationship->tablelink, $id, !@$relationship->reverse);
                $_relatives = array_unique(array_merge($lost_relatives, $current_relatives));

                foreach ($_relatives as $relative_id) {
                    $relative_linetype = $this->linetype($relationship->parent_linetype);
                    $table_name = $relative_linetype->table;

                    if (!$this->getRecord($table_name, $relative_id)) {
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

        $record = $this->getRecord($table_name, $id);
        $tableinfo = $this->config()->tables()[$table_name] ?? (object) [];

        if (@$tableinfo->format === 'binary') {
            return base64_decode($record->content);
        }

        // if ($record !== null) {
        //     $this->head = $this->db_version();
        // }

        return json_encode($record, self::ENCODING_OPTIONS);
    }

    public function blockOfRecord(string $recordId): ?Block
    {
        $version = $this->index->recordVersion($recordId);

        if ($version === null) {
            return null;
        }

        return $this->chain->getBlock($version);
    }

    public function getRecord(string $table, string $id): ?object
    {
        $block = $this->blockOfRecord($id);

        if (!$block) {
            return null;
        }

        return $block->getRecord($table, $id);
    }

    public function blockOfLink(string $linkName, string $recordId, ?bool $reverse = false): ?Block
    {
        $version = $this->index->linkVersion($linkName, $recordId, (bool) $reverse);

        if ($version === null) {
            return null;
        }

        return $this->chain->getBlock($version);
    }

    public function getLink(string $linkName, string $recordId, ?bool $reverse = false): array
    {
        if (!$block = $this->blockOfLink($linkName, $recordId, (bool) $reverse)) {
            return [];
        }

        return $block->getLink($linkName, $recordId, (bool) $reverse);
    }

    public function refresh(): string
    {
        if (!$this->verify_token($this->token())) {
            throw new BadTokenException;
        }

        // we need the index to remain the same while we work

        // TODO: resolve this major bottleneck!
        // save() and refresh() both locking index, need to reduce contention

        $this->index->safeLock(60);

        $indexHead = $this->index->head();

        $locked = [];
        $bunny = $this->index->head();
        $changed_reports = [];
        $unaffected_reports = array_flip(array_keys($this->config->reports()));

        foreach (array_keys($this->config->reports()) as $report_name) {
            $filesystem = new Filesystem();

            $report = $this->report($report_name);

            if ($report->is_fully_derived()) {
                continue;
            }

            $manager = $this
                ->reporter
                ->getManager($report_name)
                ->mkdir();

            if (!@$locked[$report_name]) {
                $manager->lock();
                $locked[$report_name] = $manager->unlock(...);
            }

            $greyhound = $manager->version();

            if ($bunny === $greyhound) {
                continue;
            }

            if (defined('JARS_VERBOSE') && JARS_VERBOSE) {
                error_log("Refreshing report $report_name [$greyhound → $bunny]");
            }

            // work out the changes

            $changes = [];
            $relatives = [];
            $metas = [];

            $this->iterateBlocks($this->chain->getBlock($greyhound)->advance(), function ($block, $version, $isHead, $isRoot) use (&$changes, &$relatives, &$metas) {
                if ($isRoot) {
                    // intial version does not contain changes; its just a link to first real block
                    return;
                }

                $metas[$version] ??= $block->meta();

                foreach ($metas[$version] as $meta) {
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
            });

            $lines_cache = [];
            $childsets = [];

            // propagate

            if ($greyhound) {
                foreach ($changes as $id => $change) {
                    $this->propagate_r($change->table, $id, $relatives, $changes);
                }
            }

            // apply the changes

            $report_affected = false;

            $listens = $report->listen();

            $listenTables = array_unique(array_filter(array_map(function ($to, $listen) {
                if (preg_match('/^report:/', $to)) {
                    return false;
                }

                return $this->linetype($to)->table;
            }, array_keys($listens), $listens)));

            $changes = array_filter($changes, fn ($change) => in_array($change->table, $listenTables));

            foreach ($changes as $id => $change) {
                foreach ($listens as $linetype_name => $listen) {
                    $report_affected = true;
                    $current_groups = [];
                    $past_groups = [];

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

                    $groups_file = $manager->dataFile($id, $linetype_name, 'groups');

                    if (in_array($change->sign, ['-', '~', '*'])) {
                        $past_groups = json_decode($filesystem->get($groups_file) ?? '[]');
                    }

                    // remove

                    foreach (array_diff($past_groups, $current_groups) as $group) {
                        $manager->delete($group, $linetype_name, $id, $filesystem);
                    }

                    // upsert

                    foreach ($current_groups as $group) {
                        $manager->upsert($group, $line, $filesystem, @$report->sorter);
                    }

                    if ($current_groups) {
                        $filesystem->put($groups_file, json_encode($current_groups, self::ENCODING_OPTIONS));
                    } elseif ($filesystem->has($groups_file)) {
                        $filesystem->delete($groups_file);
                    }

                    foreach (array_merge($past_groups, $current_groups) as $group) {
                        $changed_reports[$report_name][$group] = true;
                    }
                }
            }

            if ($report_affected) {
                $filesystem->put($manager->path('._version'), $bunny, 200);
                $filesystem->persist();

                unset($unaffected_reports[$report_name]);
            }

            unset($filesystem);
        }

        // $this->head = $bunny;

        // TODO: restore derived

        $this->refresh_derived(
            static::array_keys_recursive($changed_reports),
            $bunny,
            $unaffected_reports,
        );

        // defer saving latest version of unchanged reports, as it's not likely anyone is waiting for them

        if ($unaffected_reports) {
            $filesystem = new Filesystem();

            foreach (array_keys($unaffected_reports) as $report_name) {
                $manager = $this
                    ->reporter
                    ->getManager($report_name)
                    ->mkdir();

                if (!@$locked[$report_name]) {
                    $manager->lock();
                    $locked[$report_name] = $manager->unlock(...);
                }

                $filesystem->put($manager->path('._version'), $bunny, 200);
            }

            $filesystem->persist();

            unset($filesystem);
        }

        $this->index->unlock();

        foreach ($locked as $unlock) {
            $unlock();
        }

        return $bunny;
    }

    public function refresh_derived(array $changed, string $version, array &$unaffected_reports, array &$cache = []): void
    {
        if (!$this->verify_token($this->token())) {
            throw new BadTokenException;
        }

        $new_changed = [];

        foreach (array_keys($this->config->reports()) as $derived_reportname) {
            $derived_report = $this->report($derived_reportname);

            if (!$derived_report->is_derived()) {
                continue;
            }

            $filesystem = new Filesystem();
            $manager = $this->reporter->getManager($derived_reportname);

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
                        $cache[$change_reportname][$change_groupname] ??= $this->group($change_reportname, $change_groupname); // TODO: this used to specify $version as third argument, is this necessary? does/could it affect anything?
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

                        $group_file = $manager->path(($derived_groupname ?: '_empty') . '.json');

                        if ($exists = $cache[$derived_reportname][$derived_groupname] !== null) {
                            $filesystem->put($group_file, json_encode($cache[$derived_reportname][$derived_groupname], self::ENCODING_OPTIONS));

                            if (!in_array($derived_groupname, $groups)) {
                                $groups[] = $derived_groupname;

                                sort($groups);
                            }
                        } else {
                            $filesystem->delete($group_file);

                            if (in_array($derived_groupname, $groups)) {
                                $groups = array_values(array_diff($groups, [$derived_groupname]));
                            }
                        }

                        $manager->maintain_groups($derived_groupname, $exists, $filesystem);
                    }
                }
            }

            if ($report_affected) {
                $filesystem->put($manager->path('._version'), $version, 200); // the greyhound has caught the bunny!
                $filesystem->persist()->reset();

                unset($unaffected_reports[$derived_reportname]);
            }

            unset($filesystem);
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

            if (method_exists($report, 'init')) {
                $report->init();
            }

            $this->known['reports'][$name] = $report;
        }

        return $this->known['reports'][$name];
    }

    public function reportDataFile(string $report_name, string $id, ...$types): string
    {
        if (!$report_name) {
            throw new Exception('Report name cannot be empty');
        }

        return $this->_dataFile("reports/$report_name/.data/", $id, ...$types);
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

        // $this->head = $this->index->head();

        return $result;
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

        // $this->head = $this->db_version();

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

    private function version_number_of(string $version): int
    {
        if ($version === static::ROOT_VERSION) {
            return 0;
        }

        $file = $this->version_file($version);

        if (null === $number = $this->filesystem->get($file)) {
            throw new Exception('Could not resolve version [' . $version . '] to a number');
        }

        return intval($number);
    }

    public function versionDir(string $version): string
    {
        return $this->_dataFile('versions/', $version);
    }

    public function versionedFile(...$pieces): string
    {
        if (!$pieces) {
            throw new Exception('Versioned files must have a name, no pieces given');
        }

        return $this->versionDir() . '/' . implode('/', $pieces);
    }

    private static function writable(string $file): bool
    {
        return touch($file) && is_writable($file);
    }

    public function reindexRecord(object $record, Block $block): self
    {
        $this->index->recordVersion($record->id, $block->version());

        return $this;
    }

    public function deindexRecord(object $record): self
    {
        $this->index->recordVersion($record->id, null);

        return $this;
    }

    public function head(): string
    {
        return $this->index->head();
    }

    public function rebuildIndex(): self
    {
        if (defined('JARS_VERBOSE') && JARS_VERBOSE) {
            error_log('Rebuilding index');
        }

        $this
            ->index
            ->nuke()
            ->extendLock(600);

        for (
            $block = new Block($this->chain, self::ROOT_VERSION);
            $block !== null;
            $block = $block->advance()
        ) {
            $block->load();
            $this->index->addToChain($block);
        }

        if (defined('JARS_VERBOSE') && JARS_VERBOSE) {
            error_log('Rebuilt index');
        }

        return $this;
    }

    public function unlockIndex(): self
    {
        $this->index->unlock();

        return $this;
    }

    public function headBlock(): Block
    {
        return $this
            ->chain
            ->getBlock($this->index->head());
    }

    public function getBlock(string $version): Block
    {
        return $this->chain->getBlock($version);
    }

    public function blockExists(string $version): bool
    {
        return $this->chain->blockExists($version);
    }

    public function iterateBlocks(Block $block, ?Closure $callback = null): object
    {
        $indexHead = $this->index->head();
        $count = 0;
        $initial = $block;
        $initialVersion = $block->version();
        $final = null;

        for (
            $stop = false;
            $block !== null && !$stop;
            $block = $block->advance()
        ) {
            $count++;
            $version = $block->version();

            if ($indexHead === $version) {
                $final = $block;
                $finalVersion = $version;
                $stop = true; // don't allow the loop to go beyond the indexed chain
            }

            if ($callback) {
                $callback($block, $version, $stop, self::ROOT_VERSION === $version);
            }
        }

        return (object) compact('initialVersion', 'count', 'finalVersion', 'initial', 'final');
    }
}
