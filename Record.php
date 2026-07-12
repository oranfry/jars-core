<?php

namespace OranFry\Jars\Core;

use OranFry\Jars\Contract\Exception;

class Record
{
    static array $register = [];
    static array $proposed = [];

    private ?array $data = null;
    private bool $dirty = false;
    private bool $deleted = false;
    private ?string $file = null;
    private ?string $format = 'json';
    private ?string $id = null;
    private Jars $jars;
    private string $table;
    private int $version;

    public function __construct(Jars $jars, string $table, int $version, ?string $id)
    {
        $this->jars = $jars;
        $this->table = $table;
        $this->version = $version;
        $this->id = $id;

        if ($tableinfo = $this->jars->config()->tables()[$this->table] ?? null) {
            $this->format = @$tableinfo->format;
        }

        if ($this->id === null) {
            $this->data = [];
        }
    }

    public function __get(string $property)
    {
        if ($property == 'id' ) {
            return $this->id;
        }

        if ($this->data === null) {
            $this->load();
        }

        if (!array_key_exists($property, $this->data)) {
            return null;
        }

        return $this->data[$property];
    }

    public function __isset(string $property)
    {
        return $this->data !== null && isset($this->data[$property]);
    }

    public function __set(string $property, $value)
    {
        if (!is_null($value) && !is_scalar($value)) {
            throw new Exception('Attempt made to save non-scalar data to a record');
        }

        if ($property == 'id') {
            if (!is_string($value)) {
                throw new Exception('Attempt made to save non-string id to a record');
            }

            $this->id = $value;

            return;
        }

        if ($this->data === null) {
            $this->load();
        }

        if (!array_key_exists($property, $this->data) || $this->data[$property] !== $value) {
            $this->data[$property] = $value;
            $this->dirty = true;
        }
    }

    public function x__toString(): string
    {
        return $this->export();
    }

    public function __unset(string $property)
    {
        if ($this->data !== null) {
            unset($this->data[$property]);
        }
    }

    public function assertExistence()
    {
        if (!$this->exists()) {
            throw new Exception("import_r: No such record: {$this->table}/{$this->id}");
        }
    }

    public function currentContents(): ?string
    {
        if (!is_file($file = $this->readFile())) {
            return null;
        }

        if (false === $contents = file_get_contents($file)) {
            return null;
        }

        return $contents;
    }

    public function delete(): self
    {
        if (!$this->deleted) {
            $this->deleted = true;
            $this->dirty = true;
        }

        return $this;
    }

    public function equals(self $another)
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

    public function exists(): bool
    {
        if (!$file = $this->readFile()) {
            return false;
        }

        if (null === $contents = $this->jars->filesystem()->get($file)) {
            return false;
        }

        return $contents !== false;
    }

    private function export()
    {
        if ($this->data === null) {
            $this->load();
        }

        if ($this->deleted) {
            return false;
        }

        if ($this->format == 'binary') {
            return $this->data['content'];
        }

        return $this->data;
    }

    public function init()
    {
        $this->data = [];
    }

    private function load()
    {
        $this->assertExistence();

        $file = $this->readFile();
        $content = $this->jars->filesystem()->get($file);

        if ($this->format == 'binary') {
            $this->data = ['content' => $content];
        } else {
            if (!is_object($content)) {
                throw new Exception('Invalid JSON found in a record file [' . $file . ']');
            }

            $this->data = (array) $content;
        }
    }

    public static function of(Jars $jars, string $table, int $version, ?string $id = null): self
    {
        $key = 'existing--' . $table . '--' . $id . '--' . $version;

        return $jars->recordStore($key) ?? $jars->recordStore($key, new self($jars, $table, $version, $id));
    }

    public static function propose(Jars $jars, string $table, int $version, ?string $id = null)
    {
        $key = 'proposed--' . $table . '--' . $id . '--' . $version;

        if ($record = $jars->recordStore($key)) {
            return $record;
        }

        @unlink(self::writeFileOf($jars, $table, $version, $id)); // first clean up any previous failed attempts to update this record

        return $jars->recordStore($key, new self($jars, $table, $version, $id));
    }

    private function readFile(): ?string
    {
        if (null === $this->file) {
            if (!$this->id) {
                throw new Exception('Could not generate filename');
            }

            $bestVersion = 0;
            $dir = dirname($this->jars->dataFile($this->id));

            foreach ($this->jars->filesystem()->list($dir) as $file) {
                if (!preg_match('/^' . $this->id . '\.r\.' . $this->table . '\.([0-9]+)$/', $file, $matches)) {
                    continue;
                }

                $version = intval($matches[1]);

                if ($version > $this->version) {
                    continue;
                }

                if ($version > $bestVersion) {
                    $bestVersion = $version;
                    $this->file = $dir . '/' . $file;
                }
            }
        }

        return $this->file;
    }

    public function toArray(): array
    {
        if ($this->data === null) {
            $this->load();
        }

        return array_merge(['id' => $this->id], $this->data);
    }

    public function save(): self
    {
        if ($this->id === null) {
            throw new Exception('Tried to save record without id');
        }

        if ($this->dirty) {
            $this->jars->filesystem()->put(
                $this->writeFile(),
                $this->export(),
            );
        }

        return $this;
    }

    private function writeFile(): string
    {
        return self::writeFileOf($this->jars, $this->table, $this->version, $this->id);
    }

    public static function writeFileOf(Jars $jars, string $table, int $version, string $id): string
    {
        return $jars->dataFile($id, 'r', $table, $version);
    }
}
