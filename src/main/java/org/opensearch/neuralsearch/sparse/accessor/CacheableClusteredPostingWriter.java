/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.sparse.accessor;

import org.apache.lucene.util.BytesRef;

/**
 * Extension of the ClusteredPostingWriter interface that supports cache management operations.
 * This interface adds memory management capabilities for cached posting lists,
 * allowing implementations to free memory by removing terms and their associated
 * document clusters from the cache when they are no longer needed.
 *
 * Implementations of this interface are typically used in memory-constrained environments
 * where efficient cache management is essential for performance.
 */
public interface CacheableClusteredPostingWriter extends ClusteredPostingWriter {
    /**
     * Removes a term and its associated document clusters from the posting list.
     * This operation frees memory used by the term's posting data.
     *
     * @param term The term to be removed from the posting list
     * @return The number of RAM bytes freed by removing this term and its associated data
     */
    long erase(BytesRef term);
}
