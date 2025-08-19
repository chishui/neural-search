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

/**
 * LRU cache implementation for sparse vector caches.
 * This class manages eviction of cache entries based on least recently used policy.
 * It tracks access to terms across different indices and evicts entire term entries
 * when memory pressure requires it.
 */
@Log4j2
public class LRUTermCache extends AbstractLRUCache<LRUTermCache.TermKey> {
    private static final LRUTermCache INSTANCE = new LRUTermCache();

    private LRUTermCache() {
        super();
    }

    public static LRUTermCache getInstance() {
        return INSTANCE;
    }

    /**
     * Updates access to a term for a specific cache key.
     *
     * @param cacheKey The index cache key
     * @param term The term being accessed
     */
    public void updateAccess(CacheKey cacheKey, BytesRef term) {
        if (cacheKey == null || term == null) {
            return;
        }

        TermKey termKey = new TermKey(cacheKey, term.clone());
        super.updateAccess(termKey);
    }

    @Override
    protected long doEviction(TermKey termKey) {
        CacheKey cacheKey = termKey.getCacheKey();
        BytesRef term = termKey.getTerm();

        ClusteredPostingCacheItem clusteredPostingCacheItem = ClusteredPostingCache.getInstance().get(cacheKey);
        if (clusteredPostingCacheItem == null) {
            return 0;
        }
        return clusteredPostingCacheItem.getWriter().erase(term);
    }

    @Override
    protected void logEviction(TermKey termKey, long bytesFreed) {
        log.debug("Evicted term {} from index {}, freed {} bytes", termKey.getTerm(), termKey.getCacheKey(), bytesFreed);
    }

    @Override
    public void removeIndex(@NonNull CacheKey cacheKey) {
        accessRecencySet.removeIf(key -> key.getCacheKey().equals(cacheKey));
    }

    /**
     * Key class that combines a cache key and term for tracking LRU access.
     */
    @Getter
    @EqualsAndHashCode
    public static class TermKey {
        private final CacheKey cacheKey;
        private final BytesRef term;

        public TermKey(CacheKey cacheKey, BytesRef term) {
            this.cacheKey = cacheKey;
            this.term = term;
        }
    }
}
