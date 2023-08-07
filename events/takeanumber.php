<?php

namespace jars\events;

interface takeanumber extends \jars\Listener
{
    public function handle_takeanumber(int $pointer, string $id);
}
