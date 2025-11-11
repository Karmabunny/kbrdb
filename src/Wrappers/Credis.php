<?php

namespace karmabunny\rdb\Wrappers;

use Credis_Client;
use CredisException;
use RedisException;

/**
 * This wraps up some error handling. We're just going for simple nulls here.
 *
 * @method string|null dump(string $key)
 * @method bool restore(string $key, int $ttl, string $value, string ...$flags)
 * @method bool move(string $key, int $index)
 * @method float incrByFloat(string $key, float $amount)
 * @method int|null hStrLen(string $key, string $field)
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


    public function restore($key, $ttl, $value, ...$options)
    {
        if ($this->isStandalone()) {
            return parent::__call('restore', [$key, $ttl, $value, ...$options]);
        }
        else {
            // Because credis flattens the args.
            try {
                return $this->redis->restore($key, $ttl, $value, $options);
            } catch (RedisException $e) {
                $code = 0;
                try {
                    if (!($result = $this->redis->IsConnected())) {
                        $this->close(true);
                        $code = CredisException::CODE_DISCONNECTED;
                    }
                } catch (RedisException $e2) {
                    throw new CredisException($e2->getMessage(), $e2->getCode(), $e2);
                }
                throw new CredisException($e->getMessage(), $code, $e);
            }

        }
    }
}
