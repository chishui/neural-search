/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentWriteState;
import org.opensearch.neuralsearch.sparse.common.SparseVector;

public interface SparseVectorForwardIndex extends ForwardIndex {
    @Override
    SparseVectorForwardIndexReader getForwardIndexReader();  // covariant return type

    @Override
    SparseVectorForwardIndexWriter getForwardIndexWriter();  // covariant return type

    interface SparseVectorForwardIndexReader extends ForwardIndex.ForwardIndexReader {
        SparseVector readSparseVector(int docId);
    }

    interface SparseVectorForwardIndexWriter extends ForwardIndex.ForwardIndexWriter {
        void write(int docId, SparseVector vector);
    }

    static SparseVectorForwardIndex getOrCreate(SegmentWriteState state) {
        return InMemorySparseVectorForwardIndex.getOrCreate(state);
    }

    static void removeIndex(SegmentInfo segmentInfo) {
        InMemorySparseVectorForwardIndex.removeIndex(segmentInfo);
    }
}
