/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse;

import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import org.opensearch.index.shard.IndexShard;

/**
 * NeuralSparseIndexShard wraps IndexShard and adds methods to perform neural-sparse related operations against the shard
 */
@Log4j2
public class NeuralSparseIndexShard {
    @Getter
    private final IndexShard indexShard;
    // private final NativeMemoryCacheManager nativeMemoryCacheManager;

    /**
     * Constructor to generate NeuralSparseIndexShard. We do not perform validation that the index the shard is from
     * is in fact a neural sparse Index (index.neural_sparse = true). This may make sense to add later, but for now the operations for
     * NeuralSparseIndexShard that are not from a neural-sparse index should be no-ops.
     *
     * @param indexShard IndexShard to be wrapped.
     */
    public NeuralSparseIndexShard(IndexShard indexShard) {
        this.indexShard = indexShard;
    }

    /**
     * Return the name of the shards index
     *
     * @return Name of shard's index
     */
    public String getIndexName() {
        return indexShard.shardId().getIndexName();
    }

}
