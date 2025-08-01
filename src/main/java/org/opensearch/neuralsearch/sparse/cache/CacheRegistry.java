/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.cache;

import lombok.NonNull;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class CacheRegistry<T extends Accountable> implements Accountable {

    protected final Map<CacheKey.IndexKey, T> registryMap = new ConcurrentHashMap<>();

    /**
     * Remove a specific index from cache.
     * This method delegates to the CacheSparseVectorForwardIndex implementation
     * to clean up resources associated with the specified index key.
     *
     * @param key The IndexKey that identifies the index to be removed
     */
    public void removeIndex(@NonNull CacheKey.IndexKey key) {
        T value = registryMap.get(key);
        if (value == null) {
            return;
        }
        long ramBytesUsed = value.ramBytesUsed() + RamUsageEstimator.shallowSizeOf(key);
        registryMap.remove(key);
        CircuitBreakerManager.releaseBytes(ramBytesUsed);
    }

    /**
     * Gets an existing instance.
     *
     * @param key The IndexKey that identifies the index
     * @return The instance associated with the key, or null if not found
     */
    public T get(@NonNull CacheKey.IndexKey key) {
        return registryMap.get(key);
    }

    @Override
    public long ramBytesUsed() {
        long mem = RamUsageEstimator.shallowSizeOf(registryMap);
        for (Map.Entry<CacheKey.IndexKey, T> entry : registryMap.entrySet()) {
            mem += RamUsageEstimator.shallowSizeOf(entry.getKey());
            mem += entry.getValue().ramBytesUsed();
        }
        return mem;
    }
}
