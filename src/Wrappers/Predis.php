<?php

namespace karmabunny\rdb\Wrappers;

use Predis\Client;
use Predis\Command\CommandInterface;
use Predis\Response\ErrorInterface;
use Predis\Response\ServerException;


/**
 * This wraps up some error handling. We're just going for simple nulls here.
 *
 * Perhaps one day we'll throw (optionally) on wrongtypes. But we'll at
 * least declare a WrongTypeException instead of a generic 'ServerException'.
 *
 * @package karmabunny\rdb\Wrappers
 */
class Predis extends Client
{

    /** @inheritdoc */
    public function onErrorResponse(CommandInterface $command, ErrorInterface $response)
    {
        $this->options->exceptions = false;

        $response = parent::onErrorResponse($command, $response);

        if ($response instanceof ErrorInterface) {
            if (strpos($response->getMessage(), 'WRONGTYPE') === 0) {
                return null;
            }

            throw new ServerException($response->getMessage());
        }

        return $response;
    }
}
