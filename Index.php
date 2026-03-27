<?php

namespace OranFry\Jars\Core;

use OranFry\Jars\Contract\StaleLockException;

class Index
{
    use Lockable;

    private string $basePath;
    private Jars $jars;

    private ?string $head = null;
    private ?string $height = null;
    private ?string $locker_pin = null;

    private array $recordVersions = [];
    private $lock;

    public function __construct(Jars $jars, string $basePath)
    {
        $this->jars = $jars;
        $this->basePath = $basePath;
    }

    public function dataFile(string $id, ...$suffixes): string
    {
        return $this->jars->dataFile($this->basePath, $id, ...$suffixes);
    }

    public function head(?string $new = null): string|self
    {
        if (func_num_args()) {
            $this->head = $new;

            return $this;
        }

        if ($this->head !== null) {
            return $this->head;
        }

        $file = $this->basePath . '/head';

        return is_file($file) ? file_get_contents($file) : Jars::INITIAL_VERSION;
    }

    public function height(): int
    {
        if ($this->height !== null) {
            return $this->height;
        }

        if (!is_file($file = $this->basePath . '/height')) {
            return 0;
        }

        return $this->height = (int) file_get_contents($file);
    }

    public function recordVersion(string $id, ?string $version = null): null|string|self
    {
        if (func_num_args() > 1) {
            // echo "set $id -> $version\n";

            // foreach (debug_backtrace() as $item) {
            //     echo @$item['file'] . ':' . @$item['line'] . "\n";
            // }
            // echo "------\n\n";

            $this->recordVersions[$id] = $version;

            return $this;
        }

        if (!array_key_exists($id, $this->recordVersions)) {
            $this->recordVersions[$id] = $version = is_file($file = $this->dataFile($id, 'v')) ? file_get_contents($file) : null;

            // echo "load $id -> $version\n";

            // foreach (debug_backtrace() as $item) {
            //     echo @$item['file'] . ':' . @$item['line'] . "\n";
            // }
            // echo "------\n\n";

        }

        return $this->recordVersions[$id];
    }

    function addToChain($newBlock): self
    {
        $newVersion = $newBlock->version();

        foreach ($newBlock->recordIds() as $id) {
            Helper::mkdir(dirname($file = $this->dataFile($id, 'v')), $this->basePath);
            file_put_contents($file, $newVersion);
        }

        // $file = $this->dataFile($id, 'bm'); // block meta, maybe implement?

        $this->head = $newBlock->version();

        file_put_contents($this->basePath . '/head', $newVersion);
        file_put_contents($this->basePath . '/height', $height = $newBlock->height());

        if (defined('JARS_VERBOSE') && JARS_VERBOSE) {
            error_log("Added block to index [$newVersion] [$height]");
        }

        return $this;
    }

    public function lockFile(): string
    {
        return $this->basePath . '.lock';
    }

    public function postUnlock(): void
    {
        // unlike a block, we need to be able to lock the index time and time again
        // remove the lock file so fopen can succeed next time around

        if (defined('JARS_VERBOSE') && JARS_VERBOSE) {
            $file = $this->lockFile();
            error_log("Removing [$file]");
        }

        unlink($this->lockFile());
    }

    public function nuke(): self
    {
        $lockFile = $this->lockFile();

        `rm -f "$lockFile"`;

        $this->lock(600);

        `rm -rf "$this->basePath"`;

        mkdir($this->basePath);

        return $this;
    }

    public function safeLock(int $timeout = 10): self
    {
        try {
            $this->lock($timeout);
        } catch (StaleLockException $sle) {
            if (defined('JARS_VERBOSE') && JARS_VERBOSE) {
                error_log('Safe Lock: Handling error: ' . $sle->getMessage());
            }

            $this->jars->rebuildIndex();
        }

        return $this;
    }
}
