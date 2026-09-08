<?php

namespace karmabunny\rdb;

// @phpstan-ignore-next-line: wrong.
class_exists(\karmabunny\interfaces\JsonDeserializable::class);

// @phpstan-ignore-next-line: IBE hints.
if (false) {
    /** @deprecated Use \karmabunny\interfaces\JsonDeserializable instead */
    interface JsonDeserializable extends \karmabunny\interfaces\JsonDeserializable {}
}
