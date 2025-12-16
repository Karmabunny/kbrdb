<?php

namespace karmabunny\rdb;

use JsonSerializable;
use karmabunny\interfaces\JsonDeserializable;

/**
 * Implement this interface to create a JSON object that can be stored by Rdb.
 *
 * @see Rdb::setJsonObject()
 * @see Rdb::getJsonObject()
 */
interface RdbJsonObject extends JsonSerializable, JsonDeserializable
{
}
