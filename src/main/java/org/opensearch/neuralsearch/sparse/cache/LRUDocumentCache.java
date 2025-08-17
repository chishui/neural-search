/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.cache;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.log4j.Log4j2;
import org.opensearch.neuralsearch.sparse.accessor.SparseVectorWriter;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * LRU cache implementation for sparse vector caches.
 * This class manages eviction of cache entries based on least recently used policy.
 * It tracks access to documents across different indices and evicts entire document entries
 * when memory pressure requires it.
 */
@Log4j2
public class LRUDocumentCache {
    private static final LRUDocumentCache INSTANCE = new LRUDocumentCache();

    // Map to track document access with LRU ordering
    private final Map<DocumentKey, Boolean> accessRecencyMap;

    /**
     * The default initial capacity - MUST be a power of two.
     */
    static final int DEFAULT_INITIAL_CAPACITY = 1 << 4; // aka 16
    /**
     * The load factor used when none specified in constructor.
     */
    static final float DEFAULT_LOAD_FACTOR = 0.75f;

    private LRUDocumentCache() {
        this.accessRecencyMap = new LinkedHashMap<>(DEFAULT_INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR, true);
    }

    public static LRUDocumentCache getInstance() {
        return INSTANCE;
    }

    /**
     * Updates access to a document for a specific cache key.
     * This updates the document's position in the LRU order.
     *
     * @param cacheKey The index cache key
     * @param docId The document being accessed
     */
    public void updateAccess(CacheKey cacheKey, int docId) {
        if (cacheKey == null) {
            return;
        }

        DocumentKey documentKey = new DocumentKey(cacheKey, docId);
        synchronized (accessRecencyMap) {
            accessRecencyMap.put(documentKey, true);
        }
    }

    /**
     * Retrieves the least recently used document key without affecting its position in the access order.
     * This is useful for inspection or for implementing custom eviction policies.
     *
     * @return The least recently used TermKey, or null if the cache is empty
     */
    private DocumentKey getLeastRecentlyUsedItem() {
        synchronized (accessRecencyMap) {
            if (accessRecencyMap.isEmpty()) {
                return null;
            }

            // With accessOrder is true in the LinkedHashMap, the first entry is the least recently used
            Iterator<Map.Entry<DocumentKey, Boolean>> iterator = accessRecencyMap.entrySet().iterator();
            if (iterator.hasNext()) {
                return iterator.next().getKey();
            }
            return null;
        }
    }

    /**
     * Evicts least recently used documents from cache until the specified amount of RAM has been freed.
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
            DocumentKey leastRecentlyUsedKey = getLeastRecentlyUsedItem();

            if (leastRecentlyUsedKey == null) {
                // Cache is empty, nothing more to evict
                break;
            }

            // Evict the term and track bytes freed
            long bytesFreed = evictDocument(leastRecentlyUsedKey);

            if (bytesFreed > 0) {
                ramBytesReleased += bytesFreed;

                log.debug(
                    "Evicted document {} from index {}, freed {} bytes",
                    leastRecentlyUsedKey.getDocId(),
                    leastRecentlyUsedKey.getCacheKey(),
                    bytesFreed
                );
            }
        }

        log.debug("Freed {} bytes of memory", ramBytesReleased);
    }

    /**
     * Evicts a specific document from the forward index.
     *
     * @param documentKey The document key to evict
     * @return number of bytes freed, or 0 if the term was not evicted
     */
    private long evictDocument(DocumentKey documentKey) {
        synchronized (accessRecencyMap) {
            // Remove from access tracking
            if (accessRecencyMap.remove(documentKey) == null) {
                return 0;
            }
        }

        CacheKey cacheKey = documentKey.getCacheKey();
        int docId = documentKey.getDocId();

        // Get the caches
        ForwardIndexCacheItem forwardIndexCache = ForwardIndexCache.getInstance().get(cacheKey);
        SparseVectorWriter forwardIndexWriter = forwardIndexCache.getWriter();

        // Track bytes released
        log.debug("Evicted document {} from cache for index {}", docId, cacheKey);
        return forwardIndexWriter.erase(docId);
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
     * Key class that combines a cache key and document for tracking LRU access.
     */
    @Getter
    @EqualsAndHashCode
    private static class DocumentKey {
        private final CacheKey cacheKey;
        private final int docId;

        public DocumentKey(CacheKey cacheKey, int docId) {
            this.cacheKey = cacheKey;
            this.docId = docId;
        }
    }
}
