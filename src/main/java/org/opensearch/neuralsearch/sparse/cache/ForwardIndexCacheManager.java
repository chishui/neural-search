/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.cache;

import lombok.NonNull;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * Registry class to manage forward index within cache.
 * Given the limited cache introduced by CacheKey.IndexKey, we will simply use the addWithoutBreaking
 * method to ensure success insertion in registry.
 */
public class ForwardIndexCacheManager extends SparseCacheManager<CacheForwardIndex> {

    private static final ForwardIndexCacheManager INSTANCE = new ForwardIndexCacheManager();

    private ForwardIndexCacheManager() {
        CircuitBreakerManager.addWithoutBreaking(RamUsageEstimator.shallowSizeOf(cacheMap));
    }

    public static ForwardIndexCacheManager getInstance() {
        return INSTANCE;
    }

    @NonNull
    public CacheForwardIndex getOrCreate(@NonNull CacheKey.IndexKey key, int docCount) {
        return cacheMap.computeIfAbsent(key, k -> {
            CacheForwardIndex cacheForwardIndex = new CacheForwardIndex(docCount);
            CircuitBreakerManager.addWithoutBreaking(cacheForwardIndex.ramBytesUsed() + RamUsageEstimator.shallowSizeOf(key));
            return cacheForwardIndex;
        });
    }
}
