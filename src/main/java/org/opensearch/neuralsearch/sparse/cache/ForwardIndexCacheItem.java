/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.cache;

import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.opensearch.neuralsearch.sparse.common.SparseVector;
import org.opensearch.neuralsearch.sparse.common.SparseVectorForwardIndex;
import org.opensearch.neuralsearch.sparse.common.SparseVectorReader;
import org.opensearch.neuralsearch.sparse.common.SparseVectorWriter;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * This class is used to store/read sparse vector in cache
 */
@Log4j2
public class ForwardIndexCacheItem implements SparseVectorForwardIndex, Accountable {

    private static final String CIRCUIT_BREAKER_LABEL = "Cache Forward Index";
    private final AtomicReferenceArray<SparseVector> sparseVectors;
    private final AtomicLong usedRamBytes;
    @Getter
    private final SparseVectorReader reader = new CacheSparseVectorReader();
    @Getter
    private final SparseVectorWriter writer = new CacheSparseVectorWriter();

    public ForwardIndexCacheItem(int docCount) {
        sparseVectors = new AtomicReferenceArray<>(docCount);
        // Account for the array itself in memory usage
        usedRamBytes = new AtomicLong(RamUsageEstimator.shallowSizeOf(sparseVectors));
        CircuitBreakerManager.addWithoutBreaking(usedRamBytes.get());
    }

    @Override
    public long ramBytesUsed() {
        return usedRamBytes.get();
    }

    private class CacheSparseVectorReader implements SparseVectorReader {
        @Override
        public SparseVector read(int docId) throws IOException {
            if (docId >= sparseVectors.length()) {
                return null;
            }
            return sparseVectors.get(docId);
        }
    }

    private class CacheSparseVectorWriter implements SparseVectorWriter {

        @Override
        public void insert(int docId, SparseVector vector) {
            if (vector == null || docId >= sparseVectors.length()) {
                return;
            }

            long ramBytesUsed = vector.ramBytesUsed();

            if (!CircuitBreakerManager.addMemoryUsage(ramBytesUsed, CIRCUIT_BREAKER_LABEL)) {
                // TODO: cache eviction
                return;
            }

            if (sparseVectors.compareAndSet(docId, null, vector)) {
                usedRamBytes.addAndGet(ramBytesUsed);
            }
        }
    }
}
