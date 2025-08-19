/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.cache;

import lombok.extern.log4j.Log4j2;
import lombok.NonNull;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Set;

/**
 * Abstract LRU cache implementation for sparse vector caches.
 * This class provides common functionality for managing eviction of cache entries
 * based on least recently used policy.
 *
 * @param <LRUCacheKey> The type of key used for cache entries
 */
@Log4j2
public abstract class AbstractLRUCache<LRUCacheKey> {
    /**
     * Map to track access with LRU ordering
     */
    protected final Set<LRUCacheKey> accessRecencySet;

    /**
     * The default initial capacity - MUST be a power of two.
     */
    static final int DEFAULT_INITIAL_CAPACITY = 1 << 4; // aka 16
    /**
     * The load factor used when none specified in constructor.
     */
    static final float DEFAULT_LOAD_FACTOR = 0.75f;

    protected AbstractLRUCache() {
        this.accessRecencySet = Collections.synchronizedSet(
            Collections.newSetFromMap(new LinkedHashMap<>(DEFAULT_INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR, true))
        );
    }

    /**
     * Updates access to an item for a specific cache key.
     * This updates the item's position in the LRU order.
     *
     * @param key The key being accessed
     */
    protected void updateAccess(LRUCacheKey key) {
        if (key == null) {
            return;
        }

        accessRecencySet.add(key);
    }

    /**
     * Retrieves the least recently used key without affecting its position in the access order.
     *
     * @return The least recently used key, or null if the cache is empty
     */
    protected LRUCacheKey getLeastRecentlyUsedItem() {
        // With accessOrder is true in the LinkedHashMap, the first entry is the least recently used
        Iterator<LRUCacheKey> iterator = accessRecencySet.iterator();
        if (iterator.hasNext()) {
            return iterator.next();
        }
        return null;
    }

    /**
     * Evicts least recently used items from cache until the specified amount of RAM has been freed.
     * For thread safety, please use synchronized when calling this method
     *
     * @param ramBytesToRelease Number of bytes to evict
     */
    public void evict(long ramBytesToRelease) {
        if (ramBytesToRelease <= 0) {
            return;
        }

        long ramBytesReleased = 0;

        // Continue evicting until we've freed enough memory or the cache is empty
        while (ramBytesReleased < ramBytesToRelease) {
            // Get the least recently used item
            LRUCacheKey leastRecentlyUsedKey = getLeastRecentlyUsedItem();

            if (leastRecentlyUsedKey == null) {
                // Cache is empty, nothing more to evict
                break;
            }

            // Evict the item and track bytes freed
            long bytesFreed = evictItem(leastRecentlyUsedKey);

            if (bytesFreed > 0) {
                ramBytesReleased += bytesFreed;
                logEviction(leastRecentlyUsedKey, bytesFreed);
            }
        }

        log.debug("Freed {} bytes of memory", ramBytesReleased);
    }

    /**
     * Evicts a specific item from the cache.
     *
     * @param key The key to evict
     * @return number of bytes freed, or 0 if the item was not evicted
     */
    protected long evictItem(LRUCacheKey key) {
        if (!accessRecencySet.remove(key)) {
            return 0;
        }

        return doEviction(key);
    }

    /**
     * Performs the actual eviction of the item from cache.
     * Subclasses must implement this method to handle specific eviction logic.
     *
     * @param key The key to evict
     * @return number of bytes freed
     */
    protected abstract long doEviction(LRUCacheKey key);

    /**
     * Logs information about an evicted item.
     * Subclasses should implement this to provide appropriate logging.
     *
     * @param key The key that was evicted
     * @param bytesFreed The number of bytes freed by the eviction
     */
    protected abstract void logEviction(LRUCacheKey key, long bytesFreed);

    /**
     * Removes all entries for a specific cache key when an index is removed.
     *
     * @param cacheKey The cache key to remove
     */
    public abstract void removeIndex(@NonNull CacheKey cacheKey);
}
