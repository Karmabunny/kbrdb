<?php

namespace karmabunny\rdb;

use JsonSerializable;
use karmabunny\interfaces\JsonDeserializable;

/**
 * Implement this interface to create a JSON object that can be stored by Rdb.
 *
 * This is a convenience interface that combines JsonSerializable and JsonDeserializable.
 *
 * Use this for object drivers:
 * - HashObjectDriver
 * - JsonObjectDriver
 * - MsgPackObjectDriver
 *
 * @see Rdb::setObject()
 * @see Rdb::getObject()
 */
interface RdbJsonObject extends JsonSerializable, JsonDeserializable
{
}
