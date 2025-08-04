/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.cache;

import lombok.NonNull;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * Registry class to manage clustered posting within cache.
 * Given the limited cache introduced by CacheKey.IndexKey, we will simply use the addWithoutBreaking
 * method to ensure success insertion in registry.
 */
public class ClusteredPostingCacheManager extends SparseCacheManager<CacheClusteredPosting> {

    private static final ClusteredPostingCacheManager INSTANCE = new ClusteredPostingCacheManager();

    private ClusteredPostingCacheManager() {
        CircuitBreakerManager.addWithoutBreaking(RamUsageEstimator.shallowSizeOf(cacheMap));
    }

    public static ClusteredPostingCacheManager getInstance() {
        return INSTANCE;
    }

    @NonNull
    public CacheClusteredPosting getOrCreate(@NonNull CacheKey.IndexKey key) {
        return cacheMap.computeIfAbsent(key, k -> {
            CacheClusteredPosting cacheClusteredPosting = new CacheClusteredPosting();
            CircuitBreakerManager.addWithoutBreaking(cacheClusteredPosting.ramBytesUsed() + RamUsageEstimator.shallowSizeOf(key));
            return cacheClusteredPosting;
        });
    }
}
