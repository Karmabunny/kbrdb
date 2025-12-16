<?php

namespace karmabunny\rdb;

class_exists(\karmabunny\interfaces\JsonDeserializable::class);

// @phpstan-ignore-next-line: IBE hints.
if (false) {
    /** @deprecated Use \karmabunny\interfaces\JsonDeserializable instead */
    interface JsonDeserializable extends \karmabunny\interfaces\JsonDeserializable {}
}
