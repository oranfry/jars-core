<?php

namespace OranFry\Jars\Core;

use OranFry\Jars\Contract\Exception;
use OranFry\Jars\Contract\StaleLockException;

trait Lockable
{
    public function lock(?int $timeout = null): static
    {
        $time = time();
        $time_rendered = date('Y-m-d H:i:s', $time);

        if (null !== $this->lock) {
            throw new Exception("Unable to lock: already locked by this process");
        }

        $file = $this->lockFile();

        if (!is_dir($dir = dirname($file))) {
            @mkdir($dir);
        }

        if (!is_dir($dir)) {
            throw new Exception("Unable to lock: parent directory does not exist and could not be created");
        }

        if (is_file($file)) {
            $contents = @file_get_contents($file);
            $validTimestamp = false;

            if ($contents !== false) {
                $lines = explode("\n", $contents);

                $validTimestamp = preg_match('/^[0-9]+$/', $last = end($lines))
                    && $timestamp = intval($last);
            }

            if ($timeout !== null) {
                if (!$validTimestamp) {
                    // invalid timestamp is treated as stale
                    throw new StaleLockException("Unable to lock: lock exists with invalid expiration [$file]");
                }

                $timestamp_rendered = date('Y-m-d H:i:s', $timestamp);

                if ($timestamp > $time) {
                    $remaining = $timestamp - $time;

                    throw new Exception("Unable to lock: lock [$file] exists and not yet expired, expires in [$remaining] s, expiry [$timestamp_rendered] time [$time_rendered]");
                }

                $over = $timestamp - $time;
                throw new StaleLockException("Unable to lock: lock exists and is stale [$file], expired by [$over] s expiry [$timestamp_rendered] time [$time_rendered]");
            }

            throw new Exception("Unable to lock: lock exists belonging to another process [$file]");
        }

        if (!($this->lock = @fopen($file = $this->lockFile(), 'x'))) {
            throw new Exception("Unable to lock: could not open lock file [$file]");
        }

        if (defined('JARS_VERBOSE') && JARS_VERBOSE) {
            $rendered = date('Y-m-d H:i:s', $time + $timeout);
            error_log("Locking [$file]");
        }

        if (!flock($this->lock, LOCK_EX)) {
            fclose($this->lock);

            throw new Exception("Unable to lock: could not acquire a lock over the lock file [$file]");
        }

        if ($timeout !== null) {
            if (defined('JARS_VERBOSE') && JARS_VERBOSE) {
                $rendered = date('Y-m-d H:i:s', $time + $timeout);
                error_log("Setting lock [$file] timeout to [$rendered]");
            }

            fwrite($this->lock, $time + $timeout);
            fflush($this->lock);
        }

        return $this;
    }

    public function unlock(): static
    {
        if (null === $this->lock) {
            throw new Exception("Unable to unlock: not locked by this process");
        }

        if (defined('JARS_VERBOSE') && JARS_VERBOSE) {
            $file = $this->lockFile();
            error_log("Unlocking [$file]");
        }

        fclose($this->lock);

        $this->lock = null;

        if (method_exists($this, 'postUnlock')) {
            $this->postUnlock();
        }

        return $this;
    }

    public function extendLock(int $timeout): static
    {
        $timestamp = time() + $timeout;

        if (defined('JARS_VERBOSE') && JARS_VERBOSE) {
            $rendered = date('Y-m-d H:i:s', $timestamp);
            $file = $this->lockFile();
            error_log("Extending lock [$file] to [$rendered]");
        }

        fwrite($this->lock, "\n" . $timestamp);
        fflush($this->lock);

        return $this;
    }
}