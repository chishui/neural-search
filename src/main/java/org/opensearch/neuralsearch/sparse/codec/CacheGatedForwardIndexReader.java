/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import lombok.NonNull;
import org.opensearch.neuralsearch.sparse.common.SparseVector;
import org.opensearch.neuralsearch.sparse.common.SparseVectorForwardIndex;
import org.opensearch.neuralsearch.sparse.common.SparseVectorReader;
import org.opensearch.neuralsearch.sparse.common.SparseVectorWriter;

import java.io.IOException;

public class CacheGatedForwardIndexReader implements SparseVectorReader {
    private final SparseVectorReader inMemoryReader;
    private final SparseVectorWriter inMemoryWriter;
    // SparseBinaryDocValuesPassThrough to read forward index from lucene
    private final SparseBinaryDocValuesPassThrough luceneReader;

    public CacheGatedForwardIndexReader(
        @NonNull SparseVectorForwardIndex sparseVectorForwardIndex,
        @NonNull SparseBinaryDocValuesPassThrough luceneReader
    ) {
        this.inMemoryReader = sparseVectorForwardIndex.getReader();
        this.inMemoryWriter = sparseVectorForwardIndex.getWriter();
        this.luceneReader = luceneReader;
    }

    public SparseVector read(int docId) throws IOException {
        SparseVector vector = inMemoryReader.read(docId);
        if (vector != null) {
            return vector;
        }
        // if vector does not exist in cache, read from lucene and populate it to cache
        vector = luceneReader.read(docId);
        if (vector != null) {
            inMemoryWriter.insert(docId, vector);
        }
        return vector;
    }
}
