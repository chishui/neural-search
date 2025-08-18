/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.sparse.accessor;

/**
 * Extension of the SparseVectorWriter interface that supports cache management operations.
 * This interface adds memory management capabilities for cached sparse vectors,
 * allowing implementations to free memory by removing vectors from the cache
 * when they are no longer needed.
 *
 * Implementations of this interface are typically used in memory-constrained environments
 * where efficient cache management is essential for performance, such as in-memory
 * forward indices that need to balance memory usage with lookup speed.
 */
public interface CacheableSparseVectorWriter extends SparseVectorWriter {
    /**
     * Removes a sparse vector from the cached forward index.
     *
     * @param docId The document ID of the sparse vector to be removed from the forward index
     * @return The number of RAM bytes freed by removing this sparse vector
     */
    long erase(int docId);
}
