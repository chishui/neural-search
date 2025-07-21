/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import lombok.NonNull;
import org.opensearch.neuralsearch.sparse.common.SparseVector;
import org.opensearch.neuralsearch.sparse.common.SparseVectorReader;
import org.opensearch.neuralsearch.sparse.common.SparseVectorWriter;

import java.io.IOException;

/**
 * A cache-gated forward index reader that implements a two-tier read strategy for sparse vectors.
 *
 * This reader acts as a caching layer between clients and the underlying Lucene storage,
 * providing improved read performance through in-memory caching. It follows a read-through
 * cache pattern where cache misses are automatically populated from the underlying storage.
 */
public class CacheGatedForwardIndexReader implements SparseVectorReader {

    /** In-memory reader for fast cache lookups */
    private final SparseVectorReader inMemoryReader;

    /** In-memory writer for cache population on misses */
    private final SparseVectorWriter inMemoryWriter;

    /** Lucene-based reader for persistent storage access */
    private final SparseVectorReader luceneReader;

    /**
     * Constructs a new cache-gated forward index reader.
     *
     * @param inMemoryReader the reader for accessing cached sparse vectors in memory
     * @param inMemoryWriter the writer for populating the in-memory cache
     * @param luceneReader the reader for accessing sparse vectors from Lucene storage
     * @throws NullPointerException if any parameter is null
     */
    public CacheGatedForwardIndexReader(
        @NonNull SparseVectorReader inMemoryReader,
        @NonNull SparseVectorWriter inMemoryWriter,
        @NonNull SparseVectorReader luceneReader
    ) {
        this.inMemoryReader = inMemoryReader;
        this.inMemoryWriter = inMemoryWriter;
        this.luceneReader = luceneReader;
    }

    /**
     * Reads a sparse vector given the specified document ID.
     *
     * Read Strategy:
     * 1. First attempts to read from the in-memory cache
     * 2. On cache miss, reads from Lucene storage
     * 3. Automatically populates the cache with the retrieved vector
     *
     * @param docId the document ID for which to retrieve the sparse vector
     * @return the sparse vector associated with the document ID, or null if the vector does not exist
     * @throws IOException if an I/O error occurs while reading
     */
    public SparseVector read(int docId) throws IOException {
        SparseVector vector = inMemoryReader.read(docId);
        if (vector != null) {
            return vector;
        }

        vector = luceneReader.read(docId);
        if (vector != null) {
            inMemoryWriter.insert(docId, vector);
        }
        return vector;
    }
}
