<?php

namespace karmabunny\rdb\Wrappers;

use Credis_Client;
use CredisException;

/**
 * This wraps up some error handling. We're just going for simple nulls here.
 *
 * @method string|null dump(string $key)
 * @method bool restore(string $key, int $ttl, string $value, string ...$flags)
 * @method bool move(string $key, int $index)
 *
 * @package karmabunny\rdb\Wrappers
 */
class Credis extends Credis_Client
{

    /**
     * Are we using the binary php-redis or not?
     *
     * @return bool true if no binary, false if ising php-redis
     */
    public function isStandalone(): bool
    {
        return $this->standalone;
    }


    /** @inheritdoc */
    public function __call($name, $args)
    {
        try {
            return parent::__call($name, $args);
        }
        catch (CredisException $exception) {
            if (strpos($exception->getMessage(), 'WRONGTYPE') === 0) {
                return null;
            }

            throw $exception;
        }
    }
}
