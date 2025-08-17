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
import org.opensearch.neuralsearch.sparse.accessor.SparseVectorWriter;
import org.opensearch.neuralsearch.sparse.data.DocWeight;
import org.opensearch.neuralsearch.sparse.data.DocumentCluster;
import org.opensearch.neuralsearch.sparse.data.PostingClusters;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
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
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

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
        lock.writeLock().lock();
        try {
            accessRecencyMap.put(termKey, true);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Retrieves the least recently used term key without affecting its position in the access order.
     * This is useful for inspection or for implementing custom eviction policies.
     *
     * @return The least recently used TermKey, or null if the cache is empty
     */
    private TermKey getLeastRecentlyUsedItem() {
        lock.readLock().lock();
        try {
            if (accessRecencyMap.isEmpty()) {
                return null;
            }

            // With accessOrder is true in the LinkedHashMap, the first entry is the least recently used
            Iterator<Map.Entry<TermKey, Boolean>> iterator = accessRecencyMap.entrySet().iterator();
            if (iterator.hasNext()) {
                return iterator.next().getKey();
            }
            return null;
        } finally {
            lock.readLock().unlock();
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
        lock.writeLock().lock();
        try {
            // Remove from access tracking
            if (accessRecencyMap.remove(termKey) == null) {
                return 0;
            }

            CacheKey cacheKey = termKey.getCacheKey();
            BytesRef term = termKey.getTerm();

            // Get the caches
            ForwardIndexCacheItem forwardIndexCache = ForwardIndexCache.getInstance().get(cacheKey);
            ClusteredPostingCacheItem clusteredPostingCache = ClusteredPostingCache.getInstance().get(cacheKey);
            SparseVectorWriter forwardIndexWriter = forwardIndexCache.getWriter();

            PostingClusters postingClusters = null;
            try {
                postingClusters = clusteredPostingCache.getReader().read(term);
            } catch (IOException e) {
                log.error("Error while reading posting clusters for term {} from cache for index {}", term, cacheKey, e);
                return 0;
            }

            if (postingClusters == null) {
                return 0;
            }

            // Track bytes released
            long ramBytesReleased = 0;

            // Evict from forward index cache
            List<DocumentCluster> clusters = postingClusters.getClusters();
            for (DocumentCluster cluster : clusters) {
                Iterator<DocWeight> iterator = cluster.iterator();
                while (iterator.hasNext()) {
                    DocWeight docWeight = iterator.next();
                    ramBytesReleased += forwardIndexWriter.erase(docWeight.getDocID());
                }
            }

            // Evict from clustered posting cache
            ramBytesReleased += clusteredPostingCache.getWriter().erase(term);

            log.debug("Evicted term {} from cache for index {}", term, cacheKey);
            return ramBytesReleased;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Removes all entries for a specific cache key when an index is removed.
     *
     * @param cacheKey The cache key to remove
     */
    public void removeIndex(@NonNull CacheKey cacheKey) {
        lock.writeLock().lock();
        try {
            accessRecencyMap.keySet().removeIf(key -> key.getCacheKey().equals(cacheKey));
        } finally {
            lock.writeLock().unlock();
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
