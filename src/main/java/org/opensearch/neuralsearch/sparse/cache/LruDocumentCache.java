/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.cache;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.log4j.Log4j2;

/**
 * LRU cache implementation for sparse vector caches.
 * This class manages eviction of cache entries based on least recently used policy.
 * It tracks access to documents across different indices and evicts entire document entries
 * when memory pressure requires it.
 */
@Log4j2
public class LruDocumentCache extends AbstractLruCache<LruDocumentCache.DocumentKey> {
    private static final LruDocumentCache INSTANCE = new LruDocumentCache();

    private LruDocumentCache() {
        super();
    }

    public static LruDocumentCache getInstance() {
        return INSTANCE;
    }

    /**
     * Updates access to a term for a specific cache key.
     *
     * @param cacheKey The index cache key
     * @param docId The document id being updated
     */
    public void updateAccess(CacheKey cacheKey, int docId) {
        if (cacheKey == null) {
            return;
        }

        DocumentKey documentKey = new DocumentKey(cacheKey, docId);
        super.updateAccess(documentKey);
    }

    @Override
    protected long doEviction(DocumentKey documentKey) {
        CacheKey cacheKey = documentKey.getCacheKey();
        int docId = documentKey.getDocId();

        ForwardIndexCacheItem forwardIndexCacheItem = ForwardIndexCache.getInstance().get(cacheKey);
        if (forwardIndexCacheItem == null) {
            return 0;
        }
        return forwardIndexCacheItem.getWriter().erase(docId);
    }

    @Override
    public void removeIndex(@NonNull CacheKey cacheKey) {
        accessRecencySet.removeIf(key -> key.getCacheKey().equals(cacheKey));
    }

    /**
     * Key class that combines a cache key and document for tracking LRU access.
     */
    @Getter
    @EqualsAndHashCode
    public static class DocumentKey implements LruCacheKey {
        private final CacheKey cacheKey;
        private final int docId;

        public DocumentKey(CacheKey cacheKey, int docId) {
            this.cacheKey = cacheKey;
            this.docId = docId;
        }
    }
}
