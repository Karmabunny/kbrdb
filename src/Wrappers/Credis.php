<?php

namespace karmabunny\rdb\Wrappers;

use Credis_Client;
use CredisException;

/**
 * This wraps up some error handling. We're just going for simple nulls here.
 *
 * @package karmabunny\rdb\Wrappers
 */
class Credis extends Credis_Client
{

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
