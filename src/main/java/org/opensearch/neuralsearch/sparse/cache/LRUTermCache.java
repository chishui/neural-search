/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.cache;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.log4j.Log4j2;
import org.apache.lucene.util.BytesRef;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * LRU cache implementation for sparse vector caches.
 * This class manages eviction of cache entries based on least recently used policy.
 * It tracks access to terms across different indices and evicts entire term entries
 * when memory pressure requires it.
 */
@Log4j2
public class LRUTermCache {
    private static final LRUTermCache INSTANCE = new LRUTermCache();

    // Map to track term access with LRU ordering
    private final Map<TermKey, Boolean> accessRecencyMap;

    /**
     * The default initial capacity - MUST be a power of two.
     */
    static final int DEFAULT_INITIAL_CAPACITY = 1 << 4; // aka 16
    /**
     * The load factor used when none specified in constructor.
     */
    static final float DEFAULT_LOAD_FACTOR = 0.75f;

    private LRUTermCache() {
        this.accessRecencyMap = new LinkedHashMap<>(DEFAULT_INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR, true);
    }

    public static LRUTermCache getInstance() {
        return INSTANCE;
    }

    /**
     * Updates access to a term for a specific cache key.
     * This updates the term's position in the LRU order.
     *
     * @param cacheKey The index cache key
     * @param term The term being accessed
     */
    public void updateAccess(CacheKey cacheKey, BytesRef term) {
        if (cacheKey == null || term == null) {
            return;
        }

        TermKey termKey = new TermKey(cacheKey, term.clone());
        synchronized (accessRecencyMap) {
            accessRecencyMap.put(termKey, true);
        }
    }

    /**
     * Retrieves the least recently used term key without affecting its position in the access order.
     * This is useful for inspection or for implementing custom eviction policies.
     *
     * @return The least recently used TermKey, or null if the cache is empty
     */
    private TermKey getLeastRecentlyUsedItem() {
        synchronized (accessRecencyMap) {
            if (accessRecencyMap.isEmpty()) {
                return null;
            }

            // With accessOrder is true in the LinkedHashMap, the first entry is the least recently used
            Iterator<Map.Entry<TermKey, Boolean>> iterator = accessRecencyMap.entrySet().iterator();
            if (iterator.hasNext()) {
                return iterator.next().getKey();
            }
            return null;
        }
    }

    /**
     * Evicts least recently used terms from cache until the specified amount of RAM has been freed.
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
            // Get the least recently used term
            TermKey leastRecentlyUsedKey = getLeastRecentlyUsedItem();

            if (leastRecentlyUsedKey == null) {
                // Cache is empty, nothing more to evict
                break;
            }

            // Evict the term and track bytes freed
            long bytesFreed = evictTerm(leastRecentlyUsedKey);

            if (bytesFreed > 0) {
                ramBytesReleased += bytesFreed;

                log.debug(
                    "Evicted term {} from index {}, freed {} bytes",
                    leastRecentlyUsedKey.getTerm(),
                    leastRecentlyUsedKey.getCacheKey(),
                    bytesFreed
                );
            }
        }

        log.debug("Freed {} bytes of memory", ramBytesReleased);
    }

    /**
     * Evicts a specific term from both forward index and clustered posting caches.
     *
     * @param termKey The term key to evict
     * @return number of bytes freed, or 0 if the term was not evicted
     */
    private long evictTerm(TermKey termKey) {
        synchronized (accessRecencyMap) {
            // Remove from access tracking
            if (accessRecencyMap.remove(termKey) == null) {
                return 0;
            }
        }

        CacheKey cacheKey = termKey.getCacheKey();
        BytesRef term = termKey.getTerm();

        ClusteredPostingCacheItem clusteredPostingCache = ClusteredPostingCache.getInstance().get(cacheKey);
        log.debug("Evicted term {} from cache for index {}", term, cacheKey);
        return clusteredPostingCache.getWriter().erase(term);
    }

    /**
     * Removes all entries for a specific cache key when an index is removed.
     *
     * @param cacheKey The cache key to remove
     */
    public void removeIndex(@NonNull CacheKey cacheKey) {
        synchronized (accessRecencyMap) {
            accessRecencyMap.keySet().removeIf(key -> key.getCacheKey().equals(cacheKey));
        }
    }

    /**
     * Key class that combines a cache key and term for tracking LRU access.
     */
    @Getter
    @EqualsAndHashCode
    private static class TermKey {
        private final CacheKey cacheKey;
        private final BytesRef term;

        public TermKey(CacheKey cacheKey, BytesRef term) {
            this.cacheKey = cacheKey;
            this.term = term;
        }
    }
}
