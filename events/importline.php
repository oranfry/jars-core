<?php

namespace jars\events;

interface importline extends \jars\Listener
{
    public function handle_importline(string $table);
}
