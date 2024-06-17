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
    private $filesystem;
    private $known = [];
    private $listeners = [];
    private $token;
    private $verified_tokens = [];
    private ?string $head = null;
    private Config $config;
    private $touch_handle;
    private string $db_home;
    private ?string $locker_pin;

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

    public static function validate_password(string $password): bool
    {
        return
            is_string($password)
            && strlen($password) > 5;
    }

    public function persist(): self
    {
        $this->filesystem->persist();

        return $this;
    }

    public function login(string $username, string $password, bool $one_time = false): ?string
    {
        $start = microtime(true) * 1e6;

        try {
            if (!$this->validate_username($username)) {
                throw new BadUsernameOrPasswordException('Invalid username');
            }

            if (!$this->validate_password($password)) {
                throw new BadUsernameOrPasswordException('Invalid password');
            }

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

        $user = null;

        if (@$line->user) {
            $userfile = $this->db_home . '/current/records/users/' . $line->user . '.json';

            if (!$this->filesystem->has($userfile)) {
                throw new Exception('Token Verification Error 1', 500);
            }

            $user = json_decode($this->filesystem->get($userfile));
        }

        $token_object = (object) [
            'id' => $line->id,
            'token' => $line->token,
            'user' => @$user->id,
        ];

        $this->verified_tokens[$token] = $token_object;

        return $token_object;
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

    public function import(string $timestamp, array $lines, ?string $base_version = null, bool $dryrun = false, ?int $logging = null, bool $differential = false): array
    {
        $base_version ??= $this->head;

        if (!$this->verify_token($this->token())) {
            throw new BadTokenException;
        }

        $affecteds = [];
        $commits = [];
        $original_filesystem = clone $this->filesystem;
        $original_filesystem->freeze();

        $lines = $this->import_r($original_filesystem, $timestamp, $lines, $base_version, $affecteds, $commits, null, $logging, $differential);
        $meta  = [];

        if (
            !$dryrun
            && file_exists($current_version_file = $this->current_version_file())
            && !file_exists("{$this->db_home}/past")
        ) {
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
            $this->filesystem->reset();
        } else {
            $commits = array_filter($commits);

            $this->commit($timestamp, $commits, $meta, $base_version);
        }

        $this->trigger('entryimported');

        return $updated;
    }

    public function lock(): false|string
    {
        if ($this->touch_handle) {
            throw new Exception('Attempt to lock when already locked');
        }

        @mkdir($this->db_home, 0777, true);

        // TODO: Implement timeout using non-blocking locking in a loop until

        if (!($this->touch_handle = fopen($this->db_home . '/touch.dat', 'a'))) {
            throw new Exception('Could not open the touch file');
        }

        if (!flock($this->touch_handle, LOCK_EX)) {
            fclose($this->touch_handle);

            $this->touch_handle = null;

            throw new Exception('Could not acquire a lock over the touch file');
        }

        return $this->locker_pin = bin2hex(random_bytes(32));
    }

    private function unlock_internal(): void
    {
        $this->unlock($this->locker_pin);
    }

    public function unlock(string $locker_pin): void
    {
        if (!$this->touch_handle) {
            throw new Exception('Attempt to unlock when not locked');
        }

        if ($this->locker_pin !== $locker_pin) {
            throw new Exception('Incorrect locker PIN provided for unlocking');
        }

        $this->filesystem->persist()->reset();

        ftruncate($this->touch_handle, 0);
        fwrite($this->touch_handle, microtime(true));
        fclose($this->touch_handle);

        $this->touch_handle = null;
        $this->locker_pin = null;
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

    public function save(array $lines, ?string $base_version = null): array
    {
        if (!$this->verify_token($this->token())) {
            throw new BadTokenException;
        }

        if (!$lines) {
            return $lines;
        }

        return $this->import(date('Y-m-d H:i:s'), $lines, $base_version);
    }

    private function commit(string $timestamp, array $data, array $meta, ?string $base_version): void
    {
        foreach ($data as $id => $commit) {
            if (!count(array_diff(array_keys(get_object_vars($commit)), ['id', 'type']))) {
                unset($data[$id]);
            }
        }

        if (!count($data)) {
            return;
        }

        $has_updates = array_reduce($meta, fn ($carrie, $mathison) => $carrie || substr($mathison, 0, 1) !== '+');

        @mkdir($version_dir = $this->db_home . '/versions', 0777, true);

        $master_record_file = $this->db_home . '/master.dat';
        $master_meta_file = $master_record_file . '.meta';

        if ($i_lock = !isset($this->locker_pin)) {
            $this->lock();
        }

        $this->head = $this->db_version();

        $version_number = $this->version_number_of($this->head);

        // complain if this would cause concurrent modification

        if ($has_updates && $base_version !== $this->head) {
            if ($i_lock) {
                $this->unlock_internal();
            }

            throw new ConcurrentModificationException("Incorrect base version. Head: [$this->head], base version: [$base_version]");
        }

        // we have the floor!

        $master_export = $timestamp . ' ' . json_encode(array_values($data), JSON_UNESCAPED_SLASHES);
        $meta_export = implode(' ', $meta);

        $this->head = hash('sha256', $this->head . $master_export);

        $this->filesystem->put($this->current_version_file(), $this->head);
        $this->filesystem->put($version_dir . '/' . $this->head, $version_number + 1);
        $this->filesystem->append($master_meta_file, $this->head . ' ' . $meta_export . "\n");
        $this->filesystem->append($master_record_file, $this->head . ' ' . $master_export . "\n");

        if ($i_lock) {
            $this->unlock_internal();
        }
    }

    public function h2n(string $h): ?int
    {
        if (!$this->verify_token($this->token())) {
            throw new BadTokenException;
        }

        $sequence = $this->config->sequence();

        for ($n = 1; $n <= $sequence->max(); $n++) {
            if ($this->n2h($n) == $h) {
                return $n;
            }
        }

        return null;
    }

    public function n2h(int $n): string
    {
        if (!$this->verify_token($this->token())) {
            throw new BadTokenException;
        }

        $sequence = $this->config->sequence();

        $banned = array_unique(array_merge($sequence->banned_chars(), ['+', '/', '=']));
        $subs = $sequence->subs();

        if (isset($subs[$n])) {
            return $subs[$n];
        }

        $id = substr(str_replace($banned, '', base64_encode(hex2bin(hash('sha256', $n . '--' . $sequence->secret())))), 0, $sequence->size());

        if ($transform = $sequence->transform()) {
            $id = call_user_func($transform, $id);
        }

        return $id;
    }

    public function masterlog_check(): void
    {
        if (!$this->verify_token($this->token())) {
            throw new BadTokenException;
        }

        $master_record_file = $this->db_home . '/master.dat';
        $master_meta_file = $master_record_file . '.meta';

        if (!touch($master_record_file) || !is_writable($master_record_file)) {
            throw new Exception('Master record file not writable');
        }

        if (!touch($master_meta_file) || !is_writable($master_meta_file)) {
            throw new Exception('Master meta file not writable');
        }
    }

    public function delete(string $linetype, string $id): array
    {
        if (!$this->verify_token($this->token())) {
            throw new BadTokenException;
        }

        $this->head = $this->db_version();

        return $this->linetype($linetype)->delete($id);
    }

    public function fields(string $linetype): array
    {
        if (!$this->verify_token($this->token())) {
            throw new BadTokenException;
        }

        return $this->linetype($linetype)->fieldInfo();
    }

    public function get(string $linetype, string $id): ?object
    {
        if (!$this->verify_token($this->token())) {
            throw new BadTokenException;
        }

        $this->head = $this->db_version();

        $line = $this->linetype($linetype)->get($this->token, $id);

        if (!$line) {
            return null;
        }

        return $line;
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

    public function record(string $table, string $id, ?string &$content_type = null, ?string &$filename = null): ?string
    {
        if (!$this->verify_token($this->token())) {
            throw new BadTokenException;
        }

        $tableinfo = $this->config->tables()[$table] ?? null;
        $ext = $tableinfo->extension ?? 'json';
        $filename = $id . ($ext ? '.' . $ext : null);
        $content_type = $tableinfo->content_type ?? 'application/json';

        if (!is_file($file = $this->db_path('current/records/' . $table . '/' . $filename))) {
            return null;
        }

        $this->head = $this->db_version();

        return file_get_contents($file);
    }

    public function group(string $report, string $group = '', string|bool|null $min_version = null)
    {
        if (!$this->verify_token($this->token())) {
            throw new BadTokenException;
        }

        $group = $this
            ->report($report)
            ->get($group, $min_version === true ? $this->head : ($min_version ?: null));

        $this->head = $this->reports_version();

        return $group;
    }

    public function groups(string $report, string $prefix = '', string|bool|null $min_version = null): array
    {
        if (!$this->verify_token($this->token())) {
            throw new BadTokenException;
        }

        $groups = $this
            ->report($report)
            ->groups($prefix, $min_version === true ? $this->head : ($min_version ?: null), $version);

        $this->head = $this->reports_version();

        return $groups;
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

    public function version(): ?string
    {
        if (!$this->verify_token($this->token())) {
            throw new BadTokenException;
        }

        return $this->head;
    }

    public function config()
    {
        return $this->config;
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
                $report->init($token);
            }

            $this->known['reports'][$name] = $report;
        }

        return $this->known['reports'][$name];
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

    public function filesystem(?Filesystem $filesystem = null): null|Filesystem|self
    {
        if (func_num_args()) {
            $this->filesystem = $filesystem;

            return $this;
        }

        return $this->filesystem;
    }

    public function token(?string $token = null): self|string|null
    {
        if (func_num_args()) {
            $this->token = $token;

            return $this;
        }

        return $this->token;
    }

    public function db_path(?string $path = null): string
    {
        return $this->db_home . ($path ? '/' . $path : null);
    }

    public static function of(Config $config, string $db_home): self
    {
        return new Jars($config, $db_home);
    }

    public function get_childset(string $linetype, string $id, string $property): array
    {
        if (!$this->verify_token($this->token())) {
            throw new BadTokenException;
        }

        return $this->linetype($linetype)->get_childset($this->token, $id, $property);
    }

    private function reports_version_file(): string
    {
        return $this->db_path('reports/version.dat');
    }

    private function reports_version(&$as_number = null, &$file = null): ?string
    {
        $file = $this->reports_version_file();

        if (!$this->filesystem->has($file)) {
            $as_number = 0;

            return null;
        }

        $version = $this->filesystem->get($file);
        $as_number = (int) $this->filesystem->get($this->db_path('versions/' . $version));

        return $version;
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

        $bunny = $this->filesystem->get($this->db_path('version.dat'));
        $bunny_number = (int) $this->filesystem->get($this->db_path('versions/' . $bunny));
        $greyhound = $this->reports_version($greyhound_number);

        if ($greyhound && $bunny == $greyhound) {
            $this->head = $greyhound;

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

            [, $sign, $table, $id] = $matches;

            if (!isset($changes[$id])) {
                $changes[$id] = (object) [
                    'table' => $table,
                ];
            }

            $changes[$id]->sign = $sign;
        }

        $lines = [];
        $childsets = [];

        // propagate

        if ($greyhound) {
            foreach ($changes as $id => $change) {
                $this->propagate_r($change->table, $id, $greyhound, $changes);
            }
        }

        $changed_reports = [];

        foreach (array_keys($this->config->reports()) as $report_name) {
            $report = $this->report($report_name);

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

                    if (!is_dir(dirname($groups_file))) {
                        if (!is_dir(dirname($groups_file, 2))) {
                            mkdir(dirname($groups_file, 2));
                        }

                        mkdir(dirname($groups_file));
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
        }

        $past_dir = $this->db_path('past');

        // TODO: This assumes we never interact with past dir via Filesystem class, so implement Filesystem::find() instead

        foreach (array_filter(explode("\n", `test -d "$past_dir" && find "$past_dir" -type f || true` ?? '')) as $pastfile) {
            $this->filesystem->put($pastfile, null);
        }

        $this->filesystem->put($this->reports_version_file(), $bunny); // the greyhound has caught the bunny!

        $this->refresh_derived(static::array_keys_recursive($changed_reports), $bunny);
        $this->filesystem->persist()->reset();

        $this->head = $bunny;

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

                        $cache[$derived_reportname][$derived_groupname] = $derived_report->handle(
                            $cache[$change_reportname][$change_groupname],
                            $cache[$derived_reportname][$derived_groupname],
                            $change_reportname,
                            $change_groupname,
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
        }

        if ($new_changed) {
            $this->refresh_derived(array_keys($new_changed), $version, $cache);
        }
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

    private function load_children_r(object $line, array $children, array &$childsets): void
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

    private function propagate_r(string $table, string $id, string $version, array &$changes = [], array &$seen = []): void
    {
        foreach ($this->find_table_linetypes($table) as $linetype) {
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
                            'table' => $table,
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
            ];
        }

        usort($reports, fn ($a, $b) => $a->name <=> $b->name);

        return $reports;
    }

    public function linetypes(?string $report = null): array
    {
        if (!$this->verify_token($this->token())) {
            throw new BadTokenException;
        }

        if ($report) {
            if (!array_key_exists($report, $this->config->reports())) {
                throw new Exception('No such report [' . $report . ']');
            }

            $names = [];

            foreach ($this->report($report)->listen as $key => $value) {
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
        ], $names);

        return $linetypes;
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

    public function listen(Listener $listener): void
    {
        $this->listeners[] = $listener;
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

    public function find_table_linetypes(string $table): array
    {
        if (!$this->verify_token($this->token())) {
            throw new BadTokenException;
        }

        $found = [];

        foreach (array_keys($this->config->linetypes()) as $linetype_name) {
            $linetype = $this->linetype($linetype_name);

            if ($linetype->table === $table) {
                $found[] = $linetype;
            }
        }

        return $found;
    }

    private function version_number_of(string $version): int
    {
        if ($version === hash('sha256', 'jars')) {
            return 0;
        }

        $file = $this->db_path('versions/' . $version);

        if (null === $number = $this->filesystem->get($file)) {
            throw new Exception('Could not resolve version [' . $version . '] to a number');
        }

        return intval($number);
    }

    private function db_version(): string
    {
        return $this->filesystem->get($this->current_version_file()) ?? hash('sha256', 'jars');
    }

    private function current_version_file(): string
    {
        return $this->db_path('version.dat');
    }
}
