/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.util.BytesRef;
import org.opensearch.neuralsearch.sparse.common.SparseVector;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class InMemorySparseVectorForwardIndex implements SparseVectorForwardIndex {

    private static Map<SegmentInfo, InMemorySparseVectorForwardIndex> forwardIndexMap = new HashMap<>();

    public static InMemorySparseVectorForwardIndex getOrCreate(SegmentWriteState state) {
        SegmentInfo segmentInfo = state.segmentInfo;
        if (forwardIndexMap.containsKey(segmentInfo)) {
            return forwardIndexMap.get(segmentInfo);
        }
        InMemorySparseVectorForwardIndex inMemorySparseVectorForwardIndex = new InMemorySparseVectorForwardIndex();
        forwardIndexMap.put(segmentInfo, inMemorySparseVectorForwardIndex);
        return inMemorySparseVectorForwardIndex;
    }

    public static void removeIndex(SegmentInfo segmentInfo) {
        forwardIndexMap.remove(segmentInfo);
    }

    //
    private Map<Integer, SparseVector> sparseVectorMap = new HashMap<>();

    public InMemorySparseVectorForwardIndex() {}

    @Override
    public SparseVectorForwardIndexReader getForwardIndexReader() {
        return new InMemorySparseVectorForwardIndexReader();
    }

    @Override
    public SparseVectorForwardIndexWriter getForwardIndexWriter() {
        return new InMemorySparseVectorForwardIndexWriter();
    }

    private class InMemorySparseVectorForwardIndexReader implements SparseVectorForwardIndexReader {

        @Override
        public SparseVector readSparseVector(int docId) {
            if (sparseVectorMap.containsKey(docId)) {
                return sparseVectorMap.get(docId);
            }
            return null;
        }

        @Override
        public BytesRef read(int docId) {
            throw new UnsupportedOperationException();
        }
    }

    private class InMemorySparseVectorForwardIndexWriter implements SparseVectorForwardIndexWriter {

        @Override
        public void write(int docId, SparseVector vector) {
            if (sparseVectorMap.containsKey(docId)) {
                throw new IllegalArgumentException("docId already exists");
            }
            sparseVectorMap.put(docId, vector);
        }

        @Override
        public void write(int docId, BytesRef doc) throws IOException {
            write(docId, new SparseVector(doc));
        }

        @Override
        public void close() {
            throw new UnsupportedOperationException();
        }
    }
}
