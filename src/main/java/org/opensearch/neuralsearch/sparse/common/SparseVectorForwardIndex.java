/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.common;

import org.opensearch.neuralsearch.sparse.codec.InMemorySparseVectorForwardIndex;

/**
 * Interface for sparse vector forward index.
 * A forward index provides direct mapping from document IDs to their corresponding sparse vector representations.
 * This interface defines methods to access readers and writers for the sparse vector data,
 * as well as utility methods for index management.
 */
public interface SparseVectorForwardIndex {

    /**
     * Returns a reader for accessing sparse vectors from the forward index.
     * The reader provides methods to retrieve sparse vector data by document ID.
     *
     * @return A SparseVectorReader instance for reading sparse vector data
     */
    SparseVectorReader getReader();  // covariant return type

    /**
     * Returns a writer for adding or updating sparse vectors in the forward index.
     * The writer provides methods to store sparse vector data associated with document IDs.
     *
     * @return A SparseVectorWriter instance for writing sparse vector data
     */
    SparseVectorWriter getWriter();  // covariant return type

    /**
     * Static utility method to remove a specific index from memory.
     * This method delegates to the InMemorySparseVectorForwardIndex implementation
     * to clean up resources associated with the specified index key.
     *
     * @param key The IndexKey that identifies the index to be removed
     */
    static void removeIndex(InMemoryKey.IndexKey key) {
        InMemorySparseVectorForwardIndex.removeIndex(key);
    }
}
