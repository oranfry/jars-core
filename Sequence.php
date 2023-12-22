<?php

namespace jars;

use jars\contract\Exception;

class Sequence
{
    protected array $banned_chars = [];
    protected array $collisions = [];
    protected int $max;
    protected string $secret;
    protected int $size = 12;
    protected array $subs = [];
    protected $transform = null;

    public function __construct(string $secret, int $max)
    {
        if (strlen($secret) < 32) {
            throw new Exception('Sequence Secret too weak (32-char minimum)');
        }

        // Generate a sequence secret:
        // php -r "echo addslashes(implode('', array_map(fn () => chr(random_int(32, 126)), array_fill(0, 64, 0)))) . PHP_EOL;"

        $this->secret = $secret;
        $this->max = $max;
    }

    public function banned_chars(?array $banned_chars = null): self|Array
    {
        if (func_num_args()) {
            $this->banned_chars = $banned_chars;

            return $this;
        }

        return $this->banned_chars;
    }

    public function collisions(?array $collisions = null): self|Array
    {
        if (func_num_args()) {
            $this->collisions = $collisions;

            return $this;
        }

        return $this->collisions;
    }

    public function max(?int $max = null): self|Int
    {
        if (func_num_args()) {
            $this->max = $max;

            return $this;
        }

        return $this->max;
    }

    public function secret(?string $secret = null): self|String
    {
        if (func_num_args()) {
            $this->secret = $secret;

            return $this;
        }

        return $this->secret;
    }

    public function size(?int $size = null): self|String
    {
        if (func_num_args()) {
            $this->size = $size;

            return $this;
        }

        return $this->size;
    }

    public function subs(?array $subs = null): self|Array
    {
        if (func_num_args()) {
            $this->subs = $subs;

            return $this;
        }

        return $this->subs;
    }

    public function transform(?Callable $transform = null): self|Callable|null
    {
        if (func_num_args()) {
            $this->transform = $transform;

            return $this;
        }

        return $this->transform;
    }
}
