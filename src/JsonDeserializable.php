<?php

namespace karmabunny\rdb;

/**
 * An object that can be deserialized from a JSON array.
 */
interface JsonDeserializable
{

    /**
     * Create an object from a JSON array.
     *
     * @param array $json
     * @return self
     */
    public static function fromJson(array $json);
}
