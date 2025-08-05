/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import lombok.NonNull;
import lombok.extern.log4j.Log4j2;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.opensearch.neuralsearch.sparse.common.InMemoryKey;
import org.opensearch.neuralsearch.sparse.common.SparseVector;
import org.opensearch.neuralsearch.sparse.common.SparseVectorForwardIndex;
import org.opensearch.neuralsearch.sparse.common.SparseVectorReader;
import org.opensearch.neuralsearch.sparse.common.SparseVectorWriter;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * InMemorySparseVectorForwardIndex is used to store/read sparse vector in memory
 */
@Log4j2
public class InMemorySparseVectorForwardIndex implements SparseVectorForwardIndex, Accountable {

    private static final Map<InMemoryKey.IndexKey, InMemorySparseVectorForwardIndex> forwardIndexMap = new ConcurrentHashMap<>();

    public static long memUsage() {
        long mem = RamUsageEstimator.shallowSizeOf(forwardIndexMap);
        for (Map.Entry<InMemoryKey.IndexKey, InMemorySparseVectorForwardIndex> entry : forwardIndexMap.entrySet()) {
            mem += RamUsageEstimator.shallowSizeOf(entry.getKey());
            mem += entry.getValue().ramBytesUsed();
        }
        return mem;
    }

    @NonNull
    public static InMemorySparseVectorForwardIndex getOrCreate(InMemoryKey.IndexKey key, int docCount) {
        if (key == null) {
            throw new IllegalArgumentException("Index key cannot be null");
        }
        return forwardIndexMap.computeIfAbsent(key, k -> new InMemorySparseVectorForwardIndex(docCount));
    }

    public static InMemorySparseVectorForwardIndex get(InMemoryKey.IndexKey key) {
        if (key == null) {
            throw new IllegalArgumentException("Index key cannot be null");
        }
        return forwardIndexMap.get(key);
    }

    public static void removeIndex(InMemoryKey.IndexKey key) {
        forwardIndexMap.remove(key);
    }

    private final AtomicReferenceArray<SparseVector> sparseVectors;
    private final AtomicLong usedRamBytes = new AtomicLong(0);
    private final SparseVectorReader reader = new InMemorySparseVectorReader();
    private final SparseVectorWriter writer = new InMemorySparseVectorWriter();

    public InMemorySparseVectorForwardIndex(int docCount) {
        sparseVectors = new AtomicReferenceArray<>(docCount);
        // Account for the array itself in memory usage
        usedRamBytes.set(RamUsageEstimator.shallowSizeOf(sparseVectors));
    }

    @Override
    public SparseVectorReader getReader() {
        return reader;
    }

    @Override
    public SparseVectorWriter getWriter() {
        return writer;
    }

    @Override
    public long ramBytesUsed() {
        return usedRamBytes.get();
    }

    private class InMemorySparseVectorReader implements SparseVectorReader {
        @Override
        public SparseVector read(int docId) throws IOException {
            assert docId < sparseVectors.length() : "docId " + docId + " is out of bounds";
            return sparseVectors.get(docId);
        }
    }

    private class InMemorySparseVectorWriter implements SparseVectorWriter {

        @Override
        public void insert(int docId, SparseVector vector) {
            if (vector == null || docId >= sparseVectors.length()) {
                return;
            }

            if (sparseVectors.compareAndSet(docId, null, vector)) {
                usedRamBytes.addAndGet(vector.ramBytesUsed());
            }
        }
    }
}
