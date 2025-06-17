<?php

namespace jars;

use ReflectionFunction;
use ReflectionUnionType;
use jars\contract\Exception;
use jars\contract\LineValidationException;

class Linetype
{
    public $borrow = [];
    public $children = [];
    public $completions = [];
    public $fields = [];
    public $inlinelinks = [];
    public $name;
    public $table;
    public $unfuse_fields = [];
    public $validations = [];

    private $jars;

    private static $incoming_inlines = [];
    private static $incoming_links = [];
    private static $known = [];

    public function __construct()
    {
        // Just allow subclasses to call parent constructor
    }

    private function _unlink($token, $line, $tablelink)
    {
        $changed = [];

        foreach ($this->find_incoming_links() as $incoming) {
            if ($incoming->tablelink != $tablelink) {
                continue;
            }

            $link = Link::of($this->jars, $incoming->tablelink, $line->id, !@$incoming->reverse);

            foreach ($link->relatives() as $parent_id) {
                $parent = $this->jars->linetype($incoming->parent_linetype)->get($token, $parent_id);
                $parent->_disown = (object) [$link->property => $line->id];
                $changed[] = $parent;
            }

            return $this->save($changed);
        }

        throw new Exception('No such tablelink');
    }

    private function borrow_r($token, $line, $ignorelink = null)
    {
        // recurse to inline children

        foreach (@$this->inlinelinks ?? [] as $child) {
            if ($child->tablelink == $ignorelink) {
                continue;
            }

            if (!@$child->property) {
                throw new Exception('Inline link without property');
            }

            if (property_exists($line, $child->property)) {
                $this
                    ->jars
                    ->linetype($child->linetype)
                    ->borrow_r($token, $line->{$child->property}, $child->tablelink);
            }
        }

        foreach ($this->borrow as $field => $callback) {
            $line->{$field} = $callback($line);
        }
    }

    public function childInfo(): array
    {
        $children = [];

        foreach ($this->children as $child) {
            if (isset($child->property, $child->linetype, $child->tablelink)) {
                $children[] = (object) [
                    'property' => @$child->property,
                    'linetype' => @$child->linetype,
                ];
            }
        }

        return $children;
    }

    public function clone_r($token, $line, $ignorelinks = [])
    {
        $clone = clone $line;

        foreach ($this->children as $child) {
            if (in_array($child->tablelink, $ignorelinks)) {
                continue;
            }

            $child_ignorelinks = array_merge($ignorelinks, [$child->tablelink]);

            if (!@$child->property) {
                throw new Exception('child definition missing property: ' . $this->name);
            }

            if (!property_exists($line, $child->property) || !is_array($line->{$child->property})) {
                continue;
            }

            foreach ($line->{$child->property} as $i => $childline) {
                $line->{$child->property}[$i] = $this->jars->linetype($child->linetype)->clone_r($token, $childline, $child_ignorelinks);
            }
        }

        foreach (@$this->inlinelinks ?? [] as $child) {
            if (!@$child->property) {
                throw new Exception('Inline link without property');
            }

            $childline = @$line->{$child->property};

            if ($childline) {
                $line->{$child->property} = $this->jars->linetype($child->linetype)->clone_r($token, $childline);
            }
        }

        return $clone;
    }

    public function complete($line): void
    {
        foreach ($this->completions as $completion) {
            ($completion)($line);
        }
    }

    public function delete($id): array
    {
        $this->save([(object) [
            'id' => $id,
            '_is' => false,
        ]]);
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

    public function fieldInfo(): array
    {
        $newline_fields = $this->jars->config()->respect_newline_fields()[$this->name] ?? [];
        $download_fields = $this->jars->config()->download_fields()[$this->name] ?? [];
        $float_dp = $this->jars->config()->float_dp()[$this->name] ?? [];

        $fields = [(object) [
            'name' => 'id',
            'src' => 'builtin',
            'type' => 'string',
            'multiline' => false,
        ]];

        foreach (array_filter(array_map(fn ($link) => @$link->only_parent, $this->find_incoming_links())) as $parent_field) {
            $fields[] = (object) [
                'name' => $parent_field,
                'src' => 'parent',
                'type' => 'string',
                'multiline' => false,
            ];
        }

        foreach (['fields', 'borrow'] as $src) {
            foreach ($this->$src as $name => $callback) {
                $fieldTypeObject = (new ReflectionFunction($callback))->getReturnType();
                $fieldType = 'string';

                if ($fieldTypeObject !== null) {
                    $types = array_filter(array_map(fn ($type) => $type->getName(), match(true) {
                        $fieldTypeObject instanceof ReflectionUnionType => $fieldTypeObject->getTypes(),
                        default => [$fieldTypeObject],
                    }), fn ($name) => $name !== 'null');

                    $fieldType = reset($types) ?? 'string';

                    if ($fieldType === 'int' && in_array('float', $types)) {
                        $fieldType = 'float';
                    }
                }

                $fields[] = $field = (object) [
                    'downloadable' => false,
                    'multiline' => in_array($name, $newline_fields),
                    'name' => $name,
                    'src' => $src,
                    'type' => $fieldType,
                ];

                if ($fieldType == 'float') {
                    $field->dp = $float_dp[$name] ?? 0;
                }

                if ($download = @$download_fields[$name]) {
                    $field->downloadable = true;
                    $field->download_extension = @$download->extension;
                    $field->download_icon = @$download->icon;
                    $field->download_table = @$download->table;
                }
            }
        }

        return $fields;
    }

    public final function find_incoming_links()
    {
        if (!isset(self::$incoming_links[$this->jars->token()])) {
            self::$incoming_links[$this->jars->token()] = [];

            $linetypes = $this->jars->config()->linetypes();

            foreach ($linetypes as $name => $class) {
                $linetype = $this->jars->linetype($name);

                foreach ($linetype->children as $child) {
                    $link = clone $child;
                    $link->parent_linetype = $name;

                    self::$incoming_links[$this->jars->token()][$link->linetype][] = $link;
                }
            }

            foreach ($linetypes as $name => $class) {
                $linetype = $this->jars->linetype($name);

                foreach ($linetype->children as $child) {
                    if (@$child->only_parent) {
                        $link = (object) array_filter([
                            'tablelink' => $child->tablelink,
                            'reverse' => !($child->reverse ?? false),
                            'linetype' => $name,
                            'parent_linetype' => $child->linetype,
                        ]);

                        foreach (self::$incoming_links[$this->jars->token()][$name] ?? [] as $_link) {
                            if (
                                $link->tablelink === $_link->tablelink
                                && ($link->reverse ?? false) === ($_link->reverse ?? false)
                                && $link->linetype === $_link->linetype
                                && $link->parent_linetype === $_link->parent_linetype
                            ) {
                                continue 2;
                            }
                        }

                        self::$incoming_links[$this->jars->token()][$link->linetype][] = $link;
                    }
                }
            }
        }

        return self::$incoming_links[$this->jars->token()][$this->name] ?? [];
    }

    public final function find_incoming_inlines()
    {
        if (!isset(self::$incoming_inlines[$this->jars->token()])) {
            self::$incoming_inlines[$this->jars->token()] = [];

            foreach ($this->jars->config()->linetypes() as $name => $class) {
                $linetype = $this->jars->linetype($name);

                foreach ($linetype->inlinelinks as $child) {
                    $link = clone $child;
                    $link->parent_linetype = $name;

                    self::$incoming_inlines[$this->jars->token()][$link->linetype][] = $link;
                }
            }
        }

        return self::$incoming_inlines[$this->jars->token()][$this->name] ?? [];
    }

    public function get(?string $token, string $id, &$inlines = [])
    {
        $collected = [];

        $this->load_r($token, $id, $collected);

        $line = (object) array_map(function($callback) use ($collected) {
            return $callback($collected);
        }, $this->fields);

        $line->id = $id;
        $line->type = $this->name;

        $this->pack_r($token, $collected, $line);
        $this->only_parent_r($line);
        $this->borrow_r($token, $line);
        $inlines = $this->strip_inline_children($line);

        return $line;
    }

    public function get_childset($token, string $id, string $property, ?array &$lines_cache = null): array
    {
        $child = @array_values(array_filter($this->children, fn ($o) => $o->property == $property))[0];

        if (!$child) {
            throw new Exception('Could not find child property ' . $this->name . '->' . $property);
        }

        $childset = [];
        $link = Link::of($this->jars, $child->tablelink, $id, @$child->reverse);

        if ($child_ids = $link->relatives()) {
            $childlinetype = $this->jars->linetype($child->linetype);

            foreach ($child_ids as $child_id) {
                $childset[] = $child_line = $lines_cache[$child->linetype][$child_id] ?? $childlinetype->get($token, $child_id);

                if ($lines_cache !== null) {
                    $lines_cache[$child->linetype][$child_id] ??= $child_line;
                }
            }
        }

        return $childset;
    }

    public function has($line, $child)
    {
        return false;
    }

    public function import($token, Filesystem $original_filesystem, $timestamp, object $line, ?string $base_version, &$affecteds, &$commits, $ignorelink = null, ?int $logging = null, bool $differential = false)
    {
        $this->jars->trigger('importline', $this->table);

        $clobber = !$differential && $this->is($line); // all deletes are differential saves

        if ($clobber) {
            $fields = array_merge(
                array_keys($this->fields),
                array_keys($this->borrow),
                array_filter(array_map(fn ($l) => @$l->only_parent, $this->find_incoming_links()))
            );

            foreach ($fields as $field) {
                $line->$field ??= null;
            }
        }

        $tableinfo = $this->jars->config()->tables()[$this->table] ?? (object) [];
        $oldline = null;
        $oldrecord = null;
        $old_inlines = [];

        unset($line->_given_id);

        if ($was = (bool) @$line->id) {
            $line->_given_id = $line->id;
            $oldrecord = Record::of($this->jars, $this->table, $line->id);
            $oldline = $this->get($token, $line->id, $old_inlines);

            foreach (array_diff(array_keys(get_object_vars($oldline)), ['id', 'type']) as $property) {
                if (!property_exists($line, $property)) {
                    $line->$property = $oldline->$property;
                }
            }
        } else {
            $line->id = $this->jars->takeanumber();
        }

        $this->complete($line);

        if ($errors = $this->validate($line)) {
            throw new LineValidationException('Invalid ' . $this->name . ': ' . implode('; ', $errors));
        }

        $is = $this->is($line);

        if ($is) { // Add or Update
            $verb = $was ? '~' : '+';

            if ($logging !== null) {
                echo str_repeat(' ', $logging * 4) . $verb . '[' . $this->table . ':' . $line->id . ']' . "\n";
            }

            $record = $oldrecord ? clone $oldrecord : Record::of($this->jars, $this->table);

            if (!$was) {
                $record->created = $timestamp;
            }

            $record->modified = $timestamp;
            $record->id = $line->id;

            $affecteds[] = (object) [
                'id' => $record->id,
                'table' => $this->table,
                'action' => 'save',
                'record' => $record,
                'oldrecord' => $oldrecord,
                'oldlinks' => [],
            ];
        } else { // Remove
            if (!@$line->id) {
                throw new Exception("Missing id for deletion");
            }

            $oldrecord->assertExistence();

            if ($logging !== null) {
                echo str_repeat(' ', $logging * 4) . '-[' . $this->table . ':' . $line->id . ']' . "\n";
            }

            foreach ($this->find_incoming_links() as $parent) {
                $link = Link::of($this->jars, $parent->tablelink, $line->id, !@$parent->reverse);

                foreach ($link->relatives() as $parent_id) {
                    $affecteds[] = (object) [
                        'action' => 'disconnect',
                        'tablelink' => $parent->tablelink,
                        'left' => (@$parent->reverse ? $line->id : $parent_id),
                        'right' => (@$parent->reverse ? $parent_id : $line->id),
                    ];

                    if ($logging !== null) {
                        echo str_repeat(' ', ($logging + 1) * 4) . "<[$parent->tablelink:$parent_id,$line->id]\n";
                    }
                }
            }

            foreach ($this->children as $child) {
                $link = Link::of($this->jars, $child->tablelink, $line->id, @$child->reverse);

                if (@$child->cascade_delete) {
                    $this->jars->import_r(
                        $original_filesystem,
                        $timestamp,
                        array_map(fn ($child_id) => (object) ['type' => $child->linetype, 'id' => $child_id, '_is' => false], $link->relatives()),
                        $base_version,
                        $affecteds,
                        $commits,
                        $child->tablelink,
                        $logging !== null ? $logging + 1 : null,
                        $differential,
                    );
                } else {
                    foreach ($link->relatives() as $child_id) {
                        $affecteds[] = (object) [
                            'action' => 'disconnect',
                            'tablelink' => $child->tablelink,
                            'left' => (@$child->reverse ? $child_id : $line->id),
                            'right' => (@$child->reverse ? $line->id : $child_id),
                        ];

                        if ($logging !== null) {
                            echo str_repeat(' ', ($logging + 1) * 4) . "<[$child->tablelink:$line->id,$child_id]\n";
                        }
                    }
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
        }

        $this->strip_inline_children($line);

        if ($this->is($line)) { // Add or Update
            $this->unpack($line, $oldline, $old_inlines);
        }

        // incoming links

        foreach ($this->find_incoming_links() as $parent) {
            if (!$alias = @$parent->only_parent) {
                continue;
            }

            $parent_table = $this->jars->linetype($parent->parent_linetype)->table;

            if (@$line->$alias != @$oldline->$alias) {
                if (@$oldline->$alias) {
                    $affecteds[] = (object) [
                        'action' => 'disconnect',
                        'left' => (@$parent->reverse ? $line->id : $oldline->$alias),
                        'right' => (@$parent->reverse ? $oldline->$alias : $line->id),
                        'tablelink' => $parent->tablelink,
                        'table' => $parent_table,
                        'record' => Record::of($this->jars, $parent_table, $line->$alias),
                        'oldrecord' => Record::of($this->jars, $parent_table, $line->$alias),
                        'oldlinks' => [],
                    ];

                    if ($logging !== null) {
                        echo str_repeat(' ', ($logging + 1) * 4) . "<[$parent->tablelink:{$oldline->$alias},$line->id]\n";
                    }
                }

                if (@$line->$alias) {
                    if (!is_string($line->$alias)) {
                        throw new Exception($alias . ' should be a string containing the id of another record');
                    }

                    $affecteds[] = (object) [
                        'action' => 'connect',
                        'left' => (@$parent->reverse ? $line->id : $line->$alias),
                        'right' => (@$parent->reverse ? $line->$alias : $line->id),
                        'tablelink' => $parent->tablelink,
                        'table' => $parent_table,
                        'record' => Record::of($this->jars, $parent_table, $line->$alias),
                        'oldrecord' => Record::of($this->jars, $parent_table, $line->$alias),
                        'oldlinks' => [],
                    ];

                    if ($logging !== null) {
                        echo str_repeat(' ', ($logging + 1) * 4) . ">[$parent->tablelink:{$line->$alias},$line->id]\n";
                    }
                }
            }
        }

        // recurse to inline children

        foreach (@$this->inlinelinks ?? [] as $child) {
            if ($child->tablelink == $ignorelink) {
                continue;
            }

            if (!@$child->property) {
                throw new Exception('Inlinelink without property');
            }

            $child_linetype = $this->jars->linetype($child->linetype);
            $oldchild = null;
            $link = Link::of($this->jars, $child->tablelink, $line->id, @$child->reverse);

            if ($oldchild_id = $link->firstChild()) {
                $oldchild = Record::of($this->jars, $child_linetype->table, $oldchild_id);
                $oldchild->assertExistence();
            }

            if ($is && property_exists($line, $child->property)) { // has
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
                        throw new Exception('Given line->type is not consistent with child linetype');
                    }

                    $discard = [];

                    $childlines = $this->jars->import_r(
                        $original_filesystem,
                        $timestamp,
                        [$childline],
                        $base_version,
                        $affecteds,
                        $discard,
                        $child->tablelink,
                        $logging !== null ? $logging + 1 : null,
                        $differential,
                    );

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
                    $discard = [];

                    $child_linetype->import(
                        $token,
                        $original_filesystem,
                        $timestamp,
                        (object) ['id' => $oldchild->id, '_is' => false],
                        $base_version,
                        $affecteds,
                        $discard,
                        $child->tablelink,
                        $logging !== null ? $logging + 1 : null,
                        $differential,
                    );
                }
            }
        }

        $this->strip_inline_children($line);

        if ($is) {
            foreach ($this->unfuse_fields as $unfuse_field => $callback) {
                if (!is_callable($callback)) {
                    throw new Exception('Unfuse field set to non-callback: ' . $unfuse_field);
                }

                if (@$tableinfo->format == 'binary' && $unfuse_field != 'content') {
                    throw new Exception('disktype \'binary\' does not support unfuse other than content');
                }

                $record->{$unfuse_field} = $callback($line, $oldline);
            }
        }

        $commit = $this->clone_r($token, $line);
        $this->scrub($token, $commit);
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

    public function is(object $line): bool
    {
        return !property_exists($line, '_is') || $line->_is;
    }

    public function jars(?Jars $jars): null|Jars|self
    {
        if (func_num_args()) {
            $this->jars = $jars;

            return $this;
        }

        return $this->jars;
    }

    public function load_children(string $token, object $line, int $recursive = INF, array $ignorelinks = [])
    {
        foreach ($this->children as $child) {
            if (in_array($child->tablelink, $ignorelinks)) {
                continue;
            }

            $child_ignorelinks = array_merge($ignorelinks, [$child->tablelink]);
            $line->{$child->property} = $this->get_childset($token, $line, $child->property);

            if ($recursive > 0) {
                foreach ($childset as $childline) {
                    $childlinetype->load_children($token, $childline, $recursive - 1, $child_ignorelinks);
                }
            }
        }
    }

    protected function load_r($token, $id, &$collected, $path = '/', $ignorelink = null)
    {
        $record = Record::of($this->jars, $this->table, $id);
        $record->assertExistence();

        $collected[$path] = (object) $record->toArray();

        // recurse to inline children

        foreach (@$this->inlinelinks ?? [] as $child) {
            if ($ignorelink && $child->tablelink == $ignorelink) {
                continue;
            }

            if (!@$child->property) {
                throw new Exception('Inline link without property');
            }

            $childpath = ($path != '/' ? $path : null) . '/'  . $child->property;
            $link = Link::of($this->jars, $child->tablelink, $id, @$child->reverse);

            if ($child_ids = $link->relatives()) {
                $childlinetype = $this->jars->linetype($child->linetype);

                foreach ($child_ids as $child_id) {
                    $childlinetype->load_r($token, $child_id, $collected, $childpath, $child->tablelink);
                }
            }
        }
    }

    public function null_empty(object $line, ...$fields)
    {
        foreach ($fields as $field) {
            if (empty($line->$field)) {
                $line->field = null;
            }
        }
    }

    public function only_parent_r(object $line, ?string $ignorelink = null)
    {
        foreach ($this->find_incoming_links() as $parent) {
            if (!$alias = @$parent->only_parent) {
                continue;
            }

            $line->$alias = null;
            $link = Link::of($this->jars, $parent->tablelink, $line->id, !@$parent->reverse);

            if (!$parent_id = $link->firstChild()) {
                continue;
            }

            if (!Record::of($this->jars, $this->jars->linetype($parent->parent_linetype)->table, $parent_id)->exists()) {
                throw new Exception('Parent [' . $parent_id . '] does not exist');
            }

            $line->$alias = $parent_id;
        }

        // recurse to inline children

        foreach (@$this->inlinelinks ?? [] as $child) {
            if ($child->tablelink == $ignorelink) {
                continue;
            }

            if ($childline = @$line->{$child->property}) {
                $this
                    ->jars
                    ->linetype($child->linetype)
                    ->only_parent_r($childline, $child->tablelink);
            }
        }
    }

    private function pack_r($token, $records, $line, $path = '/', $ignorelink = null)
    {
        foreach (@$this->inlinelinks ?? [] as $child) {
            if ($child->tablelink == $ignorelink) {
                continue;
            }

            if (!@$child->property) {
                throw new Exception('Inline link without property');
            }

            $childpath = ($path != '/' ? $path : null) . '/'  . $child->property;
            $childlinetype = $this->jars->linetype($child->linetype);
            $childrecords = static::records_subset($records, $childpath);

            if ($childrecords) {
                $line->{$child->property} = (object) array_map(function($callback) use ($childrecords) {
                    return $callback($childrecords);
                }, $childlinetype->fields);

                $line->{$child->property}->id = $childrecords['/']->id;
                $line->{$child->property}->type = $child->linetype;

                $childlinetype->pack_r($token, $records, $line->{$child->property}, $childpath, $child->tablelink);
            }
        }
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

    public function recurse_to_children(
        Filesystem $original_filesystem,
        string $timestamp,
        object $line,
        ?string $base_version,
        array &$affecteds,
        array &$commits,
        ?string $ignorelink = null,
        ?int $logging = null,
        bool $differential = false
    ) {
        // recurse to normal children

        foreach ($this->children as $child) {
            $child_linetype = $this->jars->linetype($child->linetype);

            if ($childlines = @$line->{$child->property}) {
                if (!$alias = @$child->only_parent) {
                    throw new Exception('Unexpected ' . $this->name . '->' . $child->property);
                }

                $childcommits = [];

                foreach ($childlines as $childline) {
                    if (!@$childline->type) {
                        $childline->type = $child_linetype->name;
                    } elseif ($childline->type != $child_linetype->name) {
                        throw new Exception('Given line->type is not consistent with child linetype');
                    }
                }

                $childlines = $this->jars->import_r(
                    $original_filesystem,
                    $timestamp,
                    $childlines,
                    $base_version,
                    $affecteds,
                    $childcommits,
                    null,
                    $logging !== null ? $logging + 1 : null,
                    $differential,
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

            if (is_array(@$line->_adopt->{$child->property})) {
                foreach ($line->_adopt->{$child->property} as $child_id) {
                    $child_record = Record::of($this->jars, $child_linetype->table, $child_id);
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
                    $child_record = Record::of($this->jars, $child_linetype->table, $child_id);
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

    public function save($lines): array
    {
        throw new Exception('Linetype::save() used');
        foreach ($lines as $line) {
            if (!@$line->type) {
                $line->type = $this->name;
            } elseif ($line->type != $this->name) {
                throw new Exception('Given line->type inconsistent with Linetype');
            }
        }

        return $this->jars->import(date('Y-m-d H:i:s'), $lines);
    }

    public function scrub($token, $line)
    {
        if (@$line->_given_id) {
            $line->id = $line->_given_id;
        } else {
            unset($line->id);
        }

        unset($line->_given_id);

        // strip non-scalars that aren't child sets or specials

        $keep = ['_adopt', '_disown'];

        foreach (array_keys(get_object_vars($line)) as $var) {
            if ($line->$var !== null && !is_scalar($line->$var) && !in_array($var, $keep)) {
                unset($line->$var);
            }
        }

        // strip scalars that aren't known fields

        $keep = array_merge(
            ['_is', 'id'],
            array_keys($this->fields),
            array_keys($this->borrow),
            array_filter(array_map(fn($l) => @$l->only_parent, $this->find_incoming_links()))
        );

        foreach (array_keys(get_object_vars($line)) as $var) {
            if (is_scalar($line->$var) && !in_array($var, $keep)) {
                unset($line->$var);
            }
        }
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
                throw new Exception('Inline link without property: ' . $this->name . ': ' .  $child->linetype);
            }

            if (@$line->{$child->property}) {
                $stripped[$child->property] = $line->{$child->property};
            }

            unset($line->{$child->property});
        }

        return $stripped;
    }

    public function unlink($token, $line, $from)
    {
        if (!$this->jars->verify_token($token)) {
            return false;
        }

        return $this->_unlink($token, $line, $from);
    }

    public function unpack($line, $oldline, $old_inlines)
    {
    }

    public function validate($line): array
    {
        $errors = [];

        foreach ($this->validations as $validation) {
            if ($error = ($validation)($line)) {
                $errors[] = $error;
            }
        }

        return $errors;
    }
}
