<?php

namespace jars\traits;

trait simple_fields
{
    protected function simple_enum($name, $allowed, $default = null)
    {
        $this->fields[$name] = function ($records) use ($name, $allowed) : string {
            if (null === $as_string = @$allowed[$records['/']->$name]) {
                throw new Exception('Could not fuse enum "' . $this->name . '->' . $name . '" with value "' . @$records['/']->$name . '". Expected one of [' . implode(', ', array_map(fn ($v) => '"' . $v . '"', $allowed)) . ']');
            }

            return $as_string;
        };

        $this->unfuse_fields[$name] = function ($line, $oldline) use ($name, $allowed) : int {
            if (($as_int = @array_flip($allowed)[$line->$name]) === null) {
                throw new Exception('Could not unfuse enum ' . $name);
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

    protected function simple_enum_multi(string $name, array $allowed, ?string $default = null)
    {
        $this->fields[$name] = function ($records) use ($name, $allowed) : string {
            $values = [];

            foreach ($allowed as $i => $allowed_value) {
                if ($records['/']->$name & (1 << $i)) {
                    $values[] = $allowed_value;
                }
            }

            return implode(',', $values);
        };

        $this->unfuse_fields[$name] = function ($line, $oldline) use ($name, $allowed) : int {
            $as_int = 0;
            $values = $line->$name ? explode(',', $line->$name) : [];

            foreach ($allowed as $i => $allowed_value) {
                if (in_array($allowed_value, $values)) {
                    $as_int |= (1 << $i);
                }
            }

            return $as_int;
        };

        $this->validations[] = function ($line) use ($name, $allowed) : ?string {
            $values = $line->$name ? explode(',', $line->$name) : [];

            foreach ($values as $value) {
                if (!in_array($value, $allowed)) {
                    return 'invalid ' . $name . '. Unrecognised value "' . $value . '"';
                }
            }

            return null;
        };

        $default_as_int = 0;
        $default_values = $default ? explode(',', $default) : [];

        foreach ($allowed as $i => $allowed_value) {
            if (in_array($allowed_value, $default_values)) {
                $default_as_int |= (1 << $i);
            }
        }

        $this->completions[] = function ($line) use ($name, $default_as_int) {
            if (!property_exists($line, $name) || $line->$name === null) {
                $line->$name = $default_as_int;
            }
        };
    }

    protected function simple_hex(string $name, ?string $default = null)
    {
        if (@hex2bin($default) === false) {
            throw new Exception(__METHOD__ . ': default value is not valid hex');
        }

        $this->fields[$name] = $this->df_hex($name);
        $this->unfuse_fields[$name] = $this->du_hex($name);

        $this->validations[] = function ($line) use ($name) : ?string {
            if (($value = @$line->$name) && @hex2bin($value) === false) {
                return 'Invalid hexidecimal value for ' . $name;
            }

            return null;
        };

        $this->completions[] = function ($line) use ($name, $default) {
            if (!property_exists($line, $name) || $line->$name === null) {
                $line->$name = $default;
            }
        };
    }

    protected function simple_hexs()
    {
        foreach (func_get_args() as $name) {
            $this->simple_hex($name);
        }
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

    protected function df_hex(string $name)
    {
        return function($records) use ($name) : ?string {
            if (!$base64 = $records['/']->{$name}) {
                return null;
            }

            return bin2hex(base64_decode($base64));
        };
    }

    protected function du_hex(string $name)
    {
        return function($line, $oldline) use ($name) : ?string {
            if (false === $bin = @hex2bin(@$line->{$name})) {
                return null;
            }

            return base64_encode($bin);
        };
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

    protected function simple_latitude(string $name)
    {
        $this->fields[$name] = $this->df_latitude($name);
        $this->unfuse_fields[$name] = $this->du_latitude($name);
    }

    protected function df_latitude(string $name)
    {
        return fn ($records) : ?float => $records['/']->$name;
    }

    protected function du_latitude(string $name)
    {
        return fn ($line, $oldline) : ?float => is_numeric(@$line->$name) ? min(90, max(-90, (float) $line->$name)) : null;
    }

    protected function simple_longitude(string $name)
    {
        $this->fields[$name] = $this->df_longitude($name);
        $this->unfuse_fields[$name] = $this->du_longitude($name);
    }

    protected function df_longitude(string $name)
    {
        return fn ($records) : ?float => $records['/']->$name;
    }

    protected function du_longitude(string $name)
    {
        return fn ($line, $oldline) : ?float => is_numeric(@$line->$name) ? -(fmod((-min(180, max(-180, (float) $line->$name)) + 180), 360) - 180) : null;
    }

    protected function simple_float(string $name, int $dp = 2)
    {
        if ($dp < 0) {
            error_response('DP should not be negative');
        }

        if ($dp > 48) {
            error_response('Max DP is 48');
        }

        $this->fields[$name] = fn ($records) : string => bcadd('0', $records['/']->$name ?? '0', $dp);
        $this->unfuse_fields[$name] = fn ($line) : string => bcadd('0', $line->$name ?? '0', $dp);
    }

    protected function df_float(string $name)
    {
        return function($records) use ($name) : ?float {
            return $records['/']->{$name};
        };
    }

    protected function du_float(string $name)
    {
        return function($line, $oldline) use ($name) : ?float {
            if (!is_numeric(@$line->{$name})) {
                return null;
            }

            return (float) $line->{$name};
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

        throw new Exception('unsupported literal type');
    }
}
