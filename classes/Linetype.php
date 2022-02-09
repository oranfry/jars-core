<?php

namespace jars;

class Linetype
{
    public $borrow = [];
    public $children = [];
    public $completions = [];
    public $fields = [];
    public $inlinelinks = [];
    public $table;
    public $unfuse_fields = [];
    public $validations = [];

    private static $incoming_links = [];
    private static $incoming_inlines = null;
    private static $known = [];

    public function __construct()
    {
        // Just allow subclasses to call parent constructor
    }

    private function _delete($token, Filesystem $filesystem, $id)
    {
        $this->save($token, $filesystem, [(object)['id' => $id, '_is' => false]]);
    }

    private function _unlink($token, Filesystem $filesystem, $line, $tablelink)
    {
        $changed = [];

        foreach ($this->find_incoming_links($token, $filesystem) as $incoming) {
            if ($incoming->tablelink != $tablelink) {
                continue;
            }

            $link = new Link($filesystem, $incoming->tablelink, $line->id, !@$incoming->reverse);

            foreach ($link->relatives() as $parent_id) {
                $parent = static::load($token, $filesystem, $incoming->parent_linetype)->get($token, $filesystem, $parent_id);
                $parent->_disown = (object) [$link->property => $line->id];
                $changed[] = $parent;
            }

            return $this->save($token, $filesystem, $changed);
        }

        error_response('No such tablelink');
    }

    private function borrow_r($token, Filesystem $filesystem, $line, $ignorelink = null)
    {
        // recurse to inline children

        foreach (@$this->inlinelinks ?? [] as $child) {
            if ($child->tablelink == $ignorelink) {
                continue;
            }

            if (!@$child->property) {
                error_response('Inline link without property');
            }

            if (property_exists($line, $child->property)) {
                static::load($token, $filesystem, $child->linetype)->borrow_r($token, $filesystem, $line->{$child->property}, $child->tablelink);
            }
        }

        foreach ($this->borrow as $field => $callback) {
            $line->{$field} = $callback($line);
        }
    }

    public function clone_r($token, Filesystem $filesystem, $line, $ignorelinks = [])
    {
        $clone = clone $line;

        foreach ($this->children as $child) {
            if (in_array($child->tablelink, $ignorelinks)) {
                continue;
            }

            $child_ignorelinks = array_merge($ignorelinks, [$child->tablelink]);

            if (!@$child->property) {
                error_response('child definition missing property: ' . $this->name);
            }

            if (!property_exists($line, $child->property) || !is_array($line->{$child->property})) {
                continue;
            }

            foreach ($line->{$child->property} as $i => $childline) {
                $line->{$child->property}[$i] = static::load($token, $filesystem, $child->linetype)->clone_r($token, $filesystem, $childline, $child_ignorelinks);
            }
        }

        foreach (@$this->inlinelinks ?? [] as $child) {
            if (!@$child->property) {
                error_response('Inline link without property');
            }

            $childline = @$line->{$child->property};

            if ($childline) {
                $line->{$child->property} = static::load($token, $filesystem, $child->linetype)->clone_r($token, $filesystem, $childline);
            }
        }

        return $clone;
    }

    public function complete($line) : void
    {
        foreach ($this->completions as $completion) {
            ($completion)($line);
        }
    }

    public function delete($token, Filesystem $filesystem, $id)
    {
        if (!Blends::verify_token($token, $filesystem)) {
            return false;
        }

        $this->_delete($token, $filesystem, $id);
    }

    public final function find_incoming_links(?string $token, Filesystem $filesystem)
    {
        if (!isset(self::$incoming_links[$token])) {
            self::$incoming_links[$token] = [];

            foreach (BlendsConfig::get($token, $filesystem)->linetypes as $name => $class) {
                $linetype = static::load($token, $filesystem, $name);

                foreach ($linetype->children as $child) {
                    $link = clone $child;
                    $link->parent_linetype = $name;
                    self::$incoming_links[$token][$child->linetype][] = $link;
                }
            }
        }

        return @self::$incoming_links[$token][$this->name] ?? [];
    }

    public final function find_incoming_inlines($token, Filesystem $filesystem)
    {
        if (self::$incoming_inlines === null) {
            self::$incoming_inlines = [];

            foreach (BlendsConfig::get($token, $filesystem)->linetypes as $name => $class) {
                $linetype = static::load($token, $filesystem, $name);

                foreach ($linetype->inlinelinks as $child) {
                    $link = clone $child;
                    $link->parent_linetype = $name;

                    if (!isset(self::$incoming_inlines[$child->linetype])) {
                        self::$incoming_inlines[$child->linetype] = [];
                    }

                    self::$incoming_inlines[$child->linetype][] = $link;
                }
            }
        }

        return @self::$incoming_inlines[$this->name] ?: [];
    }

    public function get(?string $token, Filesystem $filesystem, string $id, &$inlines = [])
    {
        $collected = [];

        $this->load_r($token, $filesystem, $id, $collected);

        $line = (object) array_map(function($callback) use ($collected) {
            return $callback($collected);
        }, $this->fields);

        $line->id = $id;
        $line->type = $this->name;

        $this->pack_r($token, $filesystem, $collected, $line);
        $this->borrow_r($token, $filesystem, $line);
        $inlines = $this->strip_inline_children($line);

        foreach ($this->find_incoming_links($token, $filesystem) as $parent) {
            if (!$alias = @$parent->only_parent) {
                continue;
            }

            $line->$alias = null;
            $link = new Link($filesystem, $parent->tablelink, $id, !@$parent->reverse);

            if (!$parent_id = $link->firstChild()) {
                continue;
            }

            if (!Record::of($filesystem, static::load($token, $filesystem, $parent->parent_linetype)->table, $parent_id)->exists()) {
                error_response('Parent does not exist');
            }

            $line->$alias = $parent_id;
        }

        return $line;
    }

    public function import($token, Filesystem $filesystem, Filesystem $original_filesystem, $timestamp, $line, &$affecteds, &$commits, $ignorelink = null, ?int $logging = null)
    {
        $tableinfo = @BlendsConfig::get()->tables[$this->table];
        $oldline = null;
        $oldrecord = null;
        $old_inlines = [];

        if (@$line->id) {
            $line->given_id = $line->id;
            $oldrecord = Record::of($filesystem, $this->table, $line->id);
        }

        if (property_exists($line, '_is') && !$line->_is) { // Remove
            if (!@$line->id) {
                error_response("Missing id for deletion");
            }

            $is = false;
            $oldrecord->assertExistence();

            foreach ($this->find_incoming_links($token, $filesystem) as $parent) {
                $link = new Link($filesystem, $parent->tablelink, $line->id, !@$parent->reverse);

                foreach ($link->relatives() as $parent_id) {
                    $affecteds[] = (object) [
                        'action' => 'disconnect',
                        'tablelink' => $parent->tablelink,
                        'left' => (@$parent->reverse ? $line->id : $parent_id),
                        'right' => (@$parent->reverse ? $parent_id : $line->id),
                    ];
                }
            }

            foreach ($this->children as $child) {
                $link = new Link($filesystem, $child->tablelink, $line->id, @$child->reverse);

                foreach ($link->relatives() as $child_id) {
                    $affecteds[] = (object) [
                        'action' => 'disconnect',
                        'tablelink' => $child->tablelink,
                        'left' => (@$child->reverse ? $child_id : $line->id),
                        'right' => (@$child->reverse ? $line->id : $child_id),
                    ];
                }
            }

            $affecteds[] = (object) [
                'id' => $oldrecord->id,
                'table' => $this->table,
                'action' => 'delete',
                'record' => null,
                'oldrecord' => $oldrecord,
                'oldlinks' => [],
            ];
        } else { // Add or Update
            $is = true;

            if (@$line->id) {
                $oldline = $this->get($token, $original_filesystem, $line->id, $old_inlines);

                foreach (array_diff(array_keys(get_object_vars($oldline)), ['id', 'type']) as $property) {
                    if (!property_exists($line, $property)) {
                        $line->$property = $oldline->$property;
                    }
                }
            }

            $this->complete($line);
            $errors = $this->validate($line);

            if ($errors) {
                error_response('Invalid ' . $this->name . ': ' . implode('; ', $errors) . '. ' . var_export($line, 1));
            }

            if (!@$line->id) { // Add
                if (!$db_home = @Config::get()->db_home) {
                    error_response('db_home not defined', 500);
                }

                $pointer_file = $db_home . '/pointer.dat';
                $line->id = n2h($pointer = $filesystem->get($pointer_file) ?? 1);
                $filesystem->put($pointer_file, $pointer + 1);
            }

            $record = $oldrecord ? (clone $oldrecord) : Record::of($filesystem, $this->table);

            if ($oldline === null) {
                $record->created = $timestamp;
            }

            $record->id = $line->id;

            $affecteds[] = (object) [
                'id' => $record->id,
                'table' => $this->table,
                'action' => 'save',
                'record' => $record,
                'oldrecord' => $oldrecord,
                'oldlinks' => [],
            ];
        }

        $this->strip_inline_children($line);

        if (!property_exists($line, '_is') || $line->_is) { // Add or Update
            $this->unpack($line, $oldline, $old_inlines);
        }

        // incoming links

        foreach ($this->find_incoming_links($token, $filesystem) as $parent) {
            if (!$alias = @$parent->only_parent) {
                continue;
            }

            $parent_table = static::load($token, $filesystem, $parent->parent_linetype)->table;

            if (@$line->$alias != @$oldline->$alias) {
                if (@$oldline->$alias) {
                    $affecteds[] = (object) [
                        'action' => 'disconnect',
                        'left' => (@$parent->reverse ? $line->id : $oldline->$alias),
                        'right' => (@$parent->reverse ? $oldline->$alias : $line->id),
                        'tablelink' => $parent->tablelink,
                        'table' => $parent_table,
                        'record' => Record::of($filesystem, $parent_table, $line->$alias),
                        'oldrecord' => Record::of($filesystem, $parent_table, $line->$alias),
                        'oldlinks' => [],
                    ];
                }

                if (@$line->$alias) {
                    if (!is_string($line->$alias)) {
                        error_response($alias . ' should be a string containing the id of another record ' . json_encode($line->$alias));
                    }

                    $affecteds[] = (object) [
                        'action' => 'connect',
                        'left' => (@$parent->reverse ? $line->id : $line->$alias),
                        'right' => (@$parent->reverse ? $line->$alias : $line->id),
                        'tablelink' => $parent->tablelink,
                        'table' => $parent_table,
                        'record' => Record::of($filesystem, $parent_table, $line->$alias),
                        'oldrecord' => Record::of($filesystem, $parent_table, $line->$alias),
                        'oldlinks' => [],
                    ];
                }
            }
        }

        if ($logging !== null) {
            $verb = @$line->_is === false ? '-' : (@$line->given_id ? '~' : '+');
            echo str_repeat(' ', $logging * 4);
            echo $verb . '[' . $line->type . ':' . $line->id . ']';
            echo "\n";
        }

        // recurse to inline children

        foreach (@$this->inlinelinks ?? [] as $child) {
            if ($child->tablelink == $ignorelink) {
                continue;
            }

            if (!@$child->property) {
                error_response('Inlinelink without property');
            }

            $child_linetype = static::load($token, $filesystem, $child->linetype);
            $oldchild = null;
            $link = new Link($filesystem, $child->tablelink, $line->id, @$child->reverse);

            if ($oldchild_id = $link->firstChild()) {
                $oldchild = Record::of($filesystem, $child_linetype->table, $oldchild_id);
                $oldchild->assertExistence();
            }

            if (property_exists($line, $child->property)) { // has
                $childline = $line->{$child->property};

                if ($childline === 'unchanged' || $childline === 'touch') {
                    unset($line->{$child->property});
                } else {
                    if ($oldchild) { // and had
                        $childline->id = $oldchild->id;
                    }

                    if (!@$childline->type) {
                        $childline->type = $child_linetype->name;
                    } elseif ($childline->type != $child_linetype->name) {
                        error_response('Given line->type is not consistent with child linetype');
                    }

                    $discard = [];
                    $childlines = Blends::import_r($token, $filesystem, $original_filesystem, $timestamp, [$childline], $affecteds, $discard, $child->tablelink, $logging !== null ? $logging + 1 : null);
                    $affecteds[] = (object) [
                        'action' => 'connect',
                        'tablelink' => $child->tablelink,
                        'left' => (@$child->reverse ? $childlines[0]->id : $line->id),
                        'right' => (@$child->reverse ? $line->id : $childlines[0]->id),
                    ];
                }
            } elseif ($oldchild) { // had and not has
                $affecteds[] = (object) [
                    'action' => 'disconnect',
                    'tablelink' => $child->tablelink,
                    'left' => (@$child->reverse ? $oldchild->id : $line->id),
                    'right' => (@$child->reverse ? $line->id : $oldchild->id),
                ];

                if (!@$child->orphanable) {
                    $affecteds[] = (object) [
                        'action' => 'delete',
                        'id' => $oldchild->id,
                        'table' => $child_linetype->table,
                        'record' => null,
                        'oldrecord' => $oldchild,
                        'oldlinks' => [(object) [
                            'tablelink' => $child->tablelink,
                            'left' => (@$child->reverse ? $oldchild->id : $line->id),
                            'right' => (@$child->reverse ? $line->id : $oldchild->id),
                        ]],
                    ];
                }
            }
        }

        $this->strip_inline_children($line);

        if ($is) {
            foreach ($this->unfuse_fields as $unfuse_field => $callback) {
                if (!is_callable($callback)) {
                    error_response('Unfuse field set to non-callback: ' . $unfuse_field);
                }

                if (@$tableinfo->format == 'binary' && $unfuse_field != 'content') {
                    error_response('disktype \'binary\' does not support unfuse other than content');
                }

                $record->{$unfuse_field} = $callback($line, $oldline);
            }
        }

        $commit = $this->clone_r($token, $filesystem, $line);
        $this->scrub($token, $filesystem, $commit);
        $commit->type = $this->name;

        if ($oldline !== null) {
            foreach (array_diff(array_keys(get_object_vars($commit)), ['id', 'type']) as $property) {
                if (isset($oldline->$property) && is_scalar($oldline->$property) && $oldline->$property === $commit->$property) {
                    unset($commit->$property);
                }
            }
        }

        $commits[$line->id] = $commit;
    }

    public function recurse_to_children($token, Filesystem $filesystem, Filesystem $original_filesystem, $timestamp, $line, &$affecteds, &$commits, $ignorelink = null, ?int $logging = null)
    {
        // recurse to normal children

        foreach ($this->children as $child) {
            $child_linetype = static::load($token, $filesystem, $child->linetype);

            if ($alias = @$child->only_parent) {
                if ($childlines = @$line->{$child->property}) {
                    $childcommits = [];

                    foreach ($childlines as $childline) {
                        if (!@$childline->type) {
                            $childline->type = $child_linetype->name;
                        } elseif ($childline->type != $child_linetype->name) {
                            error_response('Given line->type is not consistent with child linetype');
                        }
                    }

                    $childlines = Blends::import_r(
                        $token,
                        $filesystem,
                        $original_filesystem,
                        $timestamp,
                        $childlines,
                        $affecteds,
                        $childcommits,
                        $logging !== null ? $logging + 1 : null
                    );

                    $commits[$line->id]->{$child->property} = array_filter(array_values($childcommits));

                    foreach ($childlines as $childline) {
                        $childline->$alias = $line->id;
                        $affecteds[] = (object) [
                            'action' => 'connect',
                            'tablelink' => $child->tablelink,
                            'left' => (@$child->reverse ? $childline->id : $line->id),
                            'right' => (@$child->reverse ? $line->id : $childline->id),
                        ];
                    }
                }
            } elseif (@$line->{$child->property}) {
                error_response('Unexpected ' . $this->name . '->' . $child->property);
            }

            if (is_array(@$line->_adopt->{$child->property})) {
                foreach ($line->_adopt->{$child->property} as $child_id) {
                    $child_record = Record::of($filesystem, $child_linetype->table, $child_id);
                    $child_record->assertExistence();

                    $affecteds[] = (object) [
                        'action' => 'connect',
                        'tablelink' => $child->tablelink,
                        'left' => (@$child->reverse ? $child_id : $line->id),
                        'right' => (@$child->reverse ? $line->id : $child_id),
                        'table' => $child_linetype->table,
                        'record' => $child_record,
                        'oldrecord' => $child_record,
                        'oldlinks' => [],
                    ];
                }
            }

            if (is_array(@$line->_disown->{$child->property})) {
                foreach ($line->_disown->{$child->property} as $child_id) {
                    $child_record = Record::of($filesystem, $child_linetype->table, $child_id);
                    $child_record->assertExistence();

                    $affecteds[] = (object) [
                        'action' => 'disconnect',
                        'tablelink' => $child->tablelink,
                        'left' => (@$child->reverse ? $child_id : $line->id),
                        'right' => (@$child->reverse ? $line->id : $child_id),
                        'table' => $child_linetype->table,
                        'record' => $child_record,
                        'oldrecord' => $child_record,
                        'oldlinks' => [],
                    ];
                }
            }
        }
    }

    public static function load(?string $token, Filesystem $filesystem, string $name)
    {
        if (!isset(static::$known[$name])) {
            $linetypeclass = @BlendsConfig::get($token, $filesystem)->linetypes[$name]->class;

            if (!$linetypeclass) {
                error_response("No such linetype '{$name}'");
            }

            $linetype = new $linetypeclass();
            $linetype->name = $name;

            if (method_exists($linetype, 'init')) {
                $linetype->init($token);
            }

            static::$known[$name] = $linetype;
        }

        return static::$known[$name];
    }

    protected function load_r($token, Filesystem $filesystem, $id, &$collected, $path = '/', $ignorelink = null)
    {
        $record = Record::of($filesystem, $this->table, $id);
        $record->assertExistence();

        $collected[$path] = $record;

        // recurse to inline children

        foreach (@$this->inlinelinks ?? [] as $child) {
            if ($child->tablelink == $ignorelink) {
                continue;
            }

            if (!@$child->property) {
                error_response('Inline link without property');
            }

            $childpath = ($path != '/' ? $path : null) . '/'  . $child->property;
            $link = new Link($filesystem, $child->tablelink, $id, @$child->reverse);

            if ($child_ids = $link->relatives()) {
                $childlinetype = static::load($token, $filesystem, $child->linetype);

                foreach ($child_ids as $child_id) {
                    $childlinetype->load_r($token, $filesystem, $child_id, $collected, $childpath, $child->tablelink);
                }
            }
        }
    }

    public function load_children(string $token, Filesystem $filesystem, object $line, int $recursive = INF, array $ignorelinks = [])
    {
        foreach ($this->children as $child) {
            if (in_array($child->tablelink, $ignorelinks)) {
                continue;
            }

            $child_ignorelinks = array_merge($ignorelinks, [$child->tablelink]);
            $line->{$child->property} = $this->get_childset($token, $filesystem, $line, $child->property);

            if ($recursive > 0) {
                foreach ($childset as $childline) {
                    $childlinetype->load_children($token, $filesystem, $childline, $recursive - 1, $child_ignorelinks);
                }
            }
        }
    }

    public function get_childset($token, Filesystem $filesystem, string $id, string $property)
    {
        $child = find_object($this->children, 'property', 'is', $property);

        if (!$child) {
            error_response('Could not find child property ' . $this->name . '->' . $property);
        }

        $childset = [];
        $link = new Link($filesystem, $child->tablelink, $id, @$child->reverse);

        if ($child_ids = $link->relatives()) {
            $childlinetype = static::load($token, $filesystem, $child->linetype);

            foreach ($child_ids as $child_id) {
                $childset[] = $childlinetype->get($token, $filesystem, $child_id);
            }
        }

        return $childset;
    }

    private function pack_r($token, Filesystem $filesystem, $records, $line, $path = '/', $ignorelink = null)
    {
        foreach (@$this->inlinelinks ?? [] as $child) {
            if ($child->tablelink == $ignorelink) {
                continue;
            }

            if (!@$child->property) {
                error_response('Inline link without property');
            }

            $childpath = ($path != '/' ? $path : null) . '/'  . $child->property;
            $childlinetype = static::load($token, $filesystem, $child->linetype);
            $childrecords = static::records_subset($records, $childpath);

            if ($childrecords) {
                $line->{$child->property} = (object) array_map(function($callback) use ($childrecords) {
                    return $callback($childrecords);
                }, $childlinetype->fields);

                $line->{$child->property}->id = $childrecords['/']->id;

                $childlinetype->pack_r($token, $filesystem, $records, $line->{$child->property}, $childpath, $child->tablelink);
            }
        }
    }

    public function save($token, Filesystem $filesystem, $lines)
    {
        if (!Blends::verify_token($token, $filesystem)) {
            return false;
        }

        foreach ($lines as $line) {
            if (!@$line->type) {
                $line->type = $this->name;
            } elseif ($line->type != $this->name) {
                error_response('Given line->type inconsistent with Linetype');
            }
        }

        return Blends::import($token, $filesystem, date('Y-m-d H:i:s'), $lines);
    }

    public function strip_children($line)
    {
        foreach ($this->children as $child) {
            unset($line->{$child->property});
        }
    }

    public function strip_inline_children($line)
    {
        $stripped = [];

        foreach (@$this->inlinelinks ?? [] as $child) {
            if (!@$child->property) {
                error_response('Inline link without property: ' . $this->name . ': ' .  $child->linetype);
            }

            if (@$line->{$child->property}) {
                $stripped[$child->property] = $line->{$child->property};
            }

            unset($line->{$child->property});
        }

        return $stripped;
    }

    public function scrub($token, $filesystem, $line)
    {
        if (@$line->given_id) {
            $line->id = $line->given_id;
        } else {
            unset($line->id);
        }

        unset($line->given_id);

        // strip nulls

        foreach (array_keys(get_object_vars($line)) as $var) {
            if (is_null($line->$var)) {
                unset($line->$var);
            }
        }

        // strip non-scalars that aren't child sets or specials

        $keep = ['_adopt', '_disown'];

        foreach (array_keys(get_object_vars($line)) as $var) {
            if (!is_scalar($line->$var) && !in_array($var, $keep)) {
                unset($line->$var);
            }
        }

        // strip scalars that aren't known fields

        $keep = array_merge(
            ['_is', 'id'],
            array_keys($this->fields),
            array_keys($this->borrow),
            array_filter(array_map(fn($l) => @$l->only_parent, $this->find_incoming_links($token, $filesystem)))
        );

        foreach (array_keys(get_object_vars($line)) as $var) {
            if (is_scalar($line->$var) && !in_array($var, $keep)) {
                unset($line->$var);
            }
        }
    }

    public function unlink($token, Filesystem $filesystem, $line, $from)
    {
        if (!Blends::verify_token($token, $filesystem)) {
            return false;
        }

        return $this->_unlink($token, $line, $from);
    }

    public function unpack($line, $oldline, $old_inlines)
    {
    }

    public function validate($line)
    {
        $errors = [];

        foreach ($this->validations as $validation) {
            if ($error = ($validation)($line)) {
                $errors[] = $error;
            }
        }

        return $errors;
    }

    public function has($line, $child)
    {
        return false;
    }

    protected function simple_enum($name, $allowed, $default = null)
    {
        $this->fields[$name] = function ($records) use ($name, $allowed) : string {
            if (null === $as_string = @$allowed[$records['/']->$name]) {
                error_response('Could not fuse enum "' . $this->name . '->' . $name . '" with value "' . @$records['/']->$name . '". Expected one of [' . implode(', ', array_map(fn ($v) => '"' . $v . '"', $allowed)) . ']');
            }

            return $as_string;
        };

        $this->unfuse_fields[$name] = function ($line, $oldline) use ($name, $allowed) : int {
            if (($as_int = @array_flip($allowed)[$line->$name]) === null) {
                error_response('Could not unfuse enum ' . $name);
            }

            return $as_int;
        };

        $this->validations[] = function ($line) use ($name, $allowed) : ?string {
            if (!in_array($line->$name, $allowed)) {
                return 'invalid ' . $name;
            }

            return null;
        };

        $default_index = array_search($default, $allowed);
        $default = null;

        if ($default_index !== false) {
            $default = $allowed[$default_index];
        }

        $this->completions[] = function ($line) use ($name, $default) {
            if (!property_exists($line, $name) || $line->$name === null) {
                $line->$name = $default;
            }
        };
    }

    protected function simple_int(string $name, ?int $default = null)
    {
        $this->fields[$name] = $this->df_int($name);
        $this->unfuse_fields[$name] = $this->du_int($name);

        $this->completions[] = function ($line) use ($name, $default) {
            if (!property_exists($line, $name) || $line->$name === null) {
                $line->$name = $default;
            }
        };
    }

    protected function simple_ints()
    {
        foreach (func_get_args() as $name) {
            $this->simple_int($name);
        }
    }

    protected function df_int(string $name)
    {
        return function($records) use ($name) : ?int {
            return $records['/']->{$name};
        };
    }

    protected function du_int(string $name)
    {
        return function($line, $oldline) use ($name) : ?int {
            if (!is_numeric(@$line->{$name})) {
                return null;
            }

            return (int) $line->{$name};
        };
    }

    protected function simple_string(string $name, ?string $default = null)
    {
        $this->fields[$name] = $this->df_string($name);
        $this->unfuse_fields[$name] = $this->du_string($name);

        $this->completions[] = function ($line) use ($name, $default) {
            if (!property_exists($line, $name) || $line->$name === null) {
                $line->$name = $default;
            }
        };
    }

    protected function simple_strings()
    {
        foreach (func_get_args() as $name) {
            $this->simple_string($name);
        }
    }

    protected function df_string(string $name)
    {
        return function($records) use ($name) : ?string {
            return $records['/']->{$name};
        };
    }

    protected function du_string(string $name)
    {
        return function($line, $oldline) use ($name) : ?string {
            return @$line->{$name};
        };
    }

    protected function simple_boolean(string $name, ?bool $default = null)
    {
        $this->fields[$name] = $this->df_boolean($name);
        $this->unfuse_fields[$name] = $this->du_boolean($name);

        $this->completions[] = function ($line) use ($name, $default) {
            if (!property_exists($line, $name) || $line->$name === null) {
                $line->$name = $default;
            }
        };
    }

    protected function simple_booleans()
    {
        foreach (func_get_args() as $name) {
            $this->simple_boolean($name);
        }
    }

    protected function df_boolean(string $name)
    {
        return function($records) use ($name) : ?bool {
            return @$records['/']->{$name};
        };
    }

    protected function du_boolean(string $name)
    {
        return function($line, $oldline) use ($name) : ?bool {
            return (bool) @$line->{$name};
        };
    }

    protected function literal(string $name, $value)
    {
        if (is_null($value)) {
            $this->fields[$name] = function($records) use ($value) : ?string {
                return null;
            };

            return;
        }

        if (is_string($value)) {
            $this->fields[$name] = function($records) use ($value) : string {
                return $value;
            };

            return;
        }

        if (is_bool($value)) {
            $this->fields[$name] = function($records) use ($value) : bool {
                return $value;
            };

            return;
        }

        if (is_int($value)) {
            $this->fields[$name] = function($records) use ($value) : int {
                return $value;
            };

            return;
        }

        if (is_float($value)) {
            $this->fields[$name] = function($records) use ($value) : float {
                return $value;
            };

            return;
        }

        error_response('unsupported literal type');
    }

    public function equal($record_a, $record_b)
    {
        if (is_null($record_a) !== is_null($record_b)) {
            return false;
        }

        $a_keys = array_keys(get_object_vars($record_a));
        $b_keys = array_keys(get_object_vars($record_b));

        if (array_diff($a_keys, $b_keys) || array_diff($b_keys, $a_keys)) {
            return false;
        }

        foreach ($a_keys as $key) {
            if ($record_a->$key !== $record_b->$key) {
                return false;
            }
        }

        return true;
    }

    public function fieldInfo()
    {
        $fields = [];

        foreach (['fields', 'borrow'] as $src) {
            foreach ($this->$src as $name => $callback) {
                $fieldTypeObject = (new ReflectionFunction($callback))->getReturnType();
                $fieldType = ($fieldTypeObject ? $fieldTypeObject->getName() : 'string');

                $fields[] = (object) [
                    'name' => $name,
                    'src' => $src,
                    'type' => $fieldType,
                ];
            }
        }

        return $fields;
    }

    protected function records_subset($records, $prefix)
    {
        $subset = [];

        foreach ($records as $path => $record) {
            $pattern = '/^' . preg_quote($prefix, '/') . '\b/';

            if (preg_match($pattern, $path)) {
                $subset[preg_replace($pattern, '', $path) ?: '/'] = $record;
            }
        }

        return $subset;
    }
}
