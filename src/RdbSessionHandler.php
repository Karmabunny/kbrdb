<?php
/**
 * @link      https://github.com/Karmabunny
 * @copyright Copyright (c) 2021 Karmabunny
 */

namespace karmabunny\rdb;

/**
 * A session handler for Rdb.
 *
 * Session handling can be registered using the {@see Rdb::registerSessionHandler} method.
 *
 * The php-redis adapter and credis (in non-standalone mode) offer an optional
 * 'native' handler via the `options.use_native_session` flag. This _may_ offer
 * more performance.
 */
class RdbSessionHandler implements \SessionHandlerInterface
{
    /** @var Rdb */
    protected Rdb $rdb;

    /** @var int */
    protected int $ttl;

    /** @var string */
    protected string $prefix;


    /**
     * @param Rdb $rdb
     * @param array $options [ ttl, prefix ]
     *  - ttl: seconds
     *  - prefix: default 'session:'
     */
    public function __construct(Rdb $rdb, array $options = [])
    {
        $this->rdb = $rdb;
        $this->prefix = $options['prefix'] ?? 'session:';

        $ttl = $options['ttl'] ?? ini_get('session.gc_maxlifetime');
        $this->ttl = (int) $ttl;
    }


    /** @inheritdoc*/
    public function open(string $save_path, string $session_id): bool
    {
        return true;
    }


    /** @inheritdoc*/
    public function close(): bool
    {
        return true;
    }


    /** @inheritdoc*/
    public function gc(int $maxlifetime): int
    {
        return 0;
    }


    /** @inheritdoc*/
    public function read(string $session_id): string|false
    {
        $data = $this->rdb->get($this->prefix . $session_id);
        return $data ?: '';
    }


    /** @inheritdoc*/
    public function write(string $session_id, string $session_data): bool
    {
        $this->rdb->set($this->prefix . $session_id, $session_data, $this->ttl);
        return true;
    }


    /** @inheritdoc*/
    public function destroy(string $session_id): bool
    {
        $this->rdb->del($this->prefix . $session_id);
        return true;
    }

}
