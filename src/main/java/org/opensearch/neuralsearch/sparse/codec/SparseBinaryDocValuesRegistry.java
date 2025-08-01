/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import org.apache.lucene.index.BinaryDocValues;
import org.opensearch.neuralsearch.sparse.cache.CacheKey;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SparseBinaryDocValuesRegistry {
    private static final Map<CacheKey.IndexKey, BinaryDocValues> binaryDocValuesMap = new ConcurrentHashMap<>();

    private SparseBinaryDocValuesRegistry() {}

    /**
     * Register a BinaryDocValues for a field within segment
     */
    public static void registerBinaryDocValues(CacheKey.IndexKey key, BinaryDocValues binaryDocValues) {
        binaryDocValuesMap.put(key, binaryDocValues);
    }

    /**
     * Get the BinaryDocValues for a field within segment
     */
    public static BinaryDocValues getBinaryDocValues(CacheKey.IndexKey key) {
        return binaryDocValuesMap.get(key);
    }

    /**
     * Remove a BinaryDocValues when no longer needed
     */
    public static void removeBinaryDocValues(CacheKey.IndexKey key) {
        binaryDocValuesMap.remove(key);
    }
}
