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
    // SparseTermsLuceneReader to read sparse terms from disk
    private final SparseBinaryDocValuesPassThrough sparseBinaryDocValuesPassThrough;

    public CacheGatedForwardIndexReader(
        @NonNull SparseVectorForwardIndex sparseVectorForwardIndex,
        SparseBinaryDocValuesPassThrough sparseBinaryDocValuesPassThrough
    ) {
        this.inMemoryReader = sparseVectorForwardIndex.getReader();
        this.inMemoryWriter = sparseVectorForwardIndex.getWriter();
        this.sparseBinaryDocValuesPassThrough = sparseBinaryDocValuesPassThrough;
    }

    public SparseVector read(int docId) throws IOException {
        SparseVector vector = inMemoryReader.read(docId);
        if (vector != null) {
            return vector;
        }
        vector = sparseBinaryDocValuesPassThrough.read(docId);
        inMemoryWriter.insert(docId, vector);
        return vector;
    }
}
