<?php

namespace OranFry\Jars\Core;

use OranFry\Jars\Contract\Exception;

class Block
{
    use Lockable;

    private Chain $chain;

    private string $version; // id of the block; like a commit hash
    private ?string $previous;
    private ?string $next = null;
    private ?int $height = null;
    private $lock;

    private array $records = [];

    public function __construct(Chain $chain, string $version)
    {
        $this->chain = $chain;
        $this->version = $version;
    }

    public function assertExistence(): self
    {
        if (!$this->exists()) {
            throw new Exception("Block: no such version [$this->version]");
        }

        return $this;
    }

    public function exists()
    {
        if ($this->version === Jars::INITIAL_VERSION) {
            return true; // empty base block with implicit existence
        }

        return is_dir($this->path());
    }

    public function path(?string $extra = null): string
    {
        $folder = $this->chain->dataFile($this->version);

        if ($extra !== null) {
            return $folder . '/' . $extra;
        }

        return $folder;
    }

    public function getRecord(string $table, string $id): Record
    {
        return $this->records[$id . '.' . $table] ??= new Record($this, $table, $id);
    }

    public function setRecord(string $table, string $id, Record $record): self
    {
        $this->records[$id . '.' . $table] = $record;

        return $this;
    }

    public function recordIds(): array
    {
        return array_map(fn ($record) => $record->id, $this->records);
    }

    public function version(): string
    {
        return $this->version;
    }

    public function previous(?string $previous = null): self|string|null
    {
        if (func_num_args()) {
            if ($this->version === Jars::INITIAL_VERSION) {
                throw new Exception("Block: refusing to set a previous block against the initial block");
            }

            $this->previous = $previous;

            return $this;
        }

        if ($this->version === Jars::INITIAL_VERSION) {
            return null;
        }

        if ($this->previous !== null) {
            return $this->previous;
        }

        return $this->previous = file_get_contents($this->path('previous'));
    }

    public function next(): ?string
    {
        if ($this->next !== null) {
            return $this->next;
        }

        if (is_file($file = $this->path('next'))) {
            return file_get_contents($file);
        }

        return null;
    }

    public function setNext(string $next): self
    {
        fwrite($this->lock, $this->next = $next);

        return $this;
    }

    public function load(): self
    {
        $this->previous = $this->next = $this->height = null;

        $this->previous();
        $this->next();
        $this->height();

        if ($this->version !== Jars::INITIAL_VERSION) {
            $handle = opendir($this->path());

            while ($file = readdir($handle)) {
                if (preg_match('/^r\.([a-z_]+)\.([0-9a-f]{64})$/', $file, $matches)) {
                    [, $table, $id] = $matches;
                    $this->records[$table . '.' . $id] = new Record($this, $table, $id);
                }
            }
        }

        return $this;
    }

    public function chain(): Chain
    {
        return $this->chain;
    }

    public function mkdir(): self
    {
        Helper::mkdir($this->path(), $this->chain->basePath());

        return $this;
    }

    public function recordExists(string $table, string $id): bool
    {
        if (isset($this->records[$id . '.' . $table])) {
            return true;
        }

        return Record::of($this, $table, $id)->exists();
    }

    public function save(): self
    {
        $this->mkdir();

        file_put_contents($this->path('previous'), $this->previous);
        file_put_contents($this->path('height'), $this->height);
        // file_put_contents($this->path('timestamp'), $this->timestamp);

        return $this;
    }

    public function height(?int $new = null): int|self
    {
        if (func_num_args()) {
            $this->height = $new;

            return $this;
        }

        if ($this->version === Jars::INITIAL_VERSION) {
            return 0;
        }

        if ($this->height !== null) {
            return $this->height;
        }

        return $this->height = (int) file_get_contents($this->path('height'));
    }

    public function lockFile(): string
    {
        return $this->path('next');
    }

    public function advance(): ?self
    {
        if (null === $next = $this->next()) {
            return null;
        }

        return $this->chain->getBlock($next);
    }
}